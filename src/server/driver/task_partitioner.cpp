#include "server/driver/task_partitioner.hpp"

#include "server/driver/distributed_executor.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/query_plan_analyzer.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"

namespace duckdb {

namespace {

void FlattenConjunction(const ParsedExpression &expression, ExpressionType type,
                        vector<const ParsedExpression *> &terms) {
	if (expression.GetExpressionType() == type && expression.GetExpressionClass() == ExpressionClass::CONJUNCTION) {
		for (auto &child : expression.Cast<ConjunctionExpression>().children) {
			FlattenConjunction(*child, type, terms);
		}
	} else {
		terms.push_back(&expression);
	}
}

bool ReferencesTable(const ColumnRefExpression &column, const BaseTableRef &target, TableCatalogEntry &target_table,
                     const BaseTableRef &other, TableCatalogEntry &other_table) {
	if (column.column_names.size() == 1) {
		const auto &name = column.column_names[0];
		return target_table.ColumnExists(name) && !other_table.ColumnExists(name);
	}
	if (column.column_names.size() != 2) {
		return false;
	}
	const auto &qualifier = column.column_names[0];
	const auto &target_name = target.alias.empty() ? target.table_name : target.alias;
	const auto &other_name = other.alias.empty() ? other.table_name : other.alias;
	return StringUtil::CIEquals(qualifier, target_name) && !StringUtil::CIEquals(qualifier, other_name) &&
	       target_table.ColumnExists(column.column_names[1]);
}

unique_ptr<ParsedExpression> ImpliedEqualityFilter(const ParsedExpression &where_clause, const BaseTableRef &target,
                                                  TableCatalogEntry &target_table, const BaseTableRef &other,
                                                  TableCatalogEntry &other_table) {
	if (where_clause.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return nullptr;
	}
	vector<const ParsedExpression *> branches;
	FlattenConjunction(where_clause, ExpressionType::CONJUNCTION_OR, branches);
	vector<unique_ptr<ParsedExpression>> implied;
	string chosen_column;
	for (auto *branch : branches) {
		vector<const ParsedExpression *> terms;
		FlattenConjunction(*branch, ExpressionType::CONJUNCTION_AND, terms);
		const ParsedExpression *chosen = nullptr;
		for (auto *term : terms) {
			if (term->GetExpressionClass() != ExpressionClass::COMPARISON ||
			    term->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto &comparison = term->Cast<ComparisonExpression>();
			const ParsedExpression *column = nullptr;
			if (comparison.left->GetExpressionClass() == ExpressionClass::COLUMN_REF &&
			    comparison.right->GetExpressionClass() == ExpressionClass::CONSTANT) {
				column = comparison.left.get();
			} else if (comparison.right->GetExpressionClass() == ExpressionClass::COLUMN_REF &&
			           comparison.left->GetExpressionClass() == ExpressionClass::CONSTANT) {
				column = comparison.right.get();
			}
			if (!column || !ReferencesTable(column->Cast<ColumnRefExpression>(), target, target_table, other,
			                                other_table)) {
				continue;
			}
			const auto &name = column->Cast<ColumnRefExpression>().GetColumnName();
			if (chosen_column.empty() || StringUtil::CIEquals(chosen_column, name)) {
				chosen_column = name;
				chosen = term;
				break;
			}
		}
		if (!chosen) {
			return nullptr;
		}
		implied.push_back(chosen->Copy());
	}
	return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(implied));
}

} // namespace

TaskPartitioner::TaskPartitioner(Connection &conn_p, QueryPlanAnalyzer &analyzer_p)
    : conn(conn_p), analyzer(analyzer_p) {
}

vector<DistributedPipelineTask> TaskPartitioner::CreateSingleTask(const string &base_sql) {
	vector<DistributedPipelineTask> tasks;
	DistributedPipelineTask task;
	task.task_id = 0;
	task.total_tasks = 1;
	task.task_sql = base_sql;
	task.row_group_start = 0;
	task.row_group_end = 0;
	tasks.emplace_back(std::move(task));
	return tasks;
}

vector<DistributedPipelineTask> TaskPartitioner::ExtractOrderKeyRangeTasks(const string &sql, idx_t num_workers) {
	auto reject = [](const char *reason) {
		if (std::getenv("DUCKHERDER_PROFILE_DISTRIBUTED")) {
			std::fprintf(stderr, "PROFILE q3_range rejected=%s\n", reason);
		}
		return vector<DistributedPipelineTask> {};
	};
	if (num_workers < 2) {
		return reject("workers");
	}
	// Only read l_orderkey row-group metadata. pragma_storage_info materializes every column segment.
	vector<int64_t> lows;
	int64_t previous_hi = -1;
	bool ordered = true;
	bool has_nulls = false;
	try {
		conn.context->RunFunctionInTransaction([&]() {
			TableCatalogEntry *lineitem = nullptr;
			for (const auto &name : {"customer", "orders", "lineitem"}) {
				auto &entry = Catalog::GetEntry(*conn.context, INVALID_CATALOG, DEFAULT_SCHEMA,
				                                EntryLookupInfo(CatalogType::TABLE_ENTRY, name));
				auto &table = entry.Cast<TableCatalogEntry>();
				if (!table.IsDuckTable() || !StringUtil::CIEquals(table.catalog.GetName(), "object_db") ||
				    !StringUtil::CIEquals(table.schema.name, "main")) {
					ordered = false;
					return;
				}
				if (StringUtil::CIEquals(name, "lineitem")) {
					lineitem = &table;
				}
			}
			if (!lineitem || !lineitem->ColumnExists("l_orderkey")) {
				ordered = false;
				return;
			}
			const auto &column = lineitem->GetColumn("l_orderkey");
			if (column.Type() != LogicalType::INTEGER && column.Type() != LogicalType::BIGINT) {
				ordered = false;
				return;
			}
			auto row_groups = lineitem->GetStorage().GetRowGroupCollection()->GetRowGroups();
			for (auto segment = row_groups->GetRootSegment(); segment; segment = row_groups->GetNextSegment(*segment)) {
				auto stats = segment->GetNode().GetStatistics(column.StorageOid());
				if (!stats || !NumericStats::HasMinMax(*stats)) {
					ordered = false;
					return;
				}
				has_nulls |= stats->CanHaveNull();
				const auto lo = NumericStats::Min(*stats).GetValue<int64_t>();
				const auto hi = NumericStats::Max(*stats).GetValue<int64_t>();
				if (lo < 0 || hi < lo || (!lows.empty() && lo < previous_hi)) {
					ordered = false;
					return;
				}
				lows.push_back(lo);
				previous_hi = hi;
			}
		});
	} catch (const Exception &) {
		return reject("stats_error");
	}
	if (!ordered || lows.size() < num_workers) {
		return reject("stats");
	}
	vector<int64_t> cutoffs;
	for (idx_t worker = 1; worker < num_workers; ++worker) {
		const auto cutoff = lows[lows.size() * worker / num_workers];
		if (cutoff <= lows.front() || (!cutoffs.empty() && cutoff <= cutoffs.back())) {
			return reject("cutoff");
		}
		cutoffs.push_back(cutoff);
	}
	if (std::getenv("DUCKHERDER_PROFILE_DISTRIBUTED")) {
		std::fprintf(stderr, "PROFILE q3_range groups=%llu tasks=%llu\n",
		             static_cast<unsigned long long>(lows.size()), static_cast<unsigned long long>(num_workers));
	}
	vector<DistributedPipelineTask> tasks;
	tasks.reserve(num_workers);
	for (idx_t worker = 0; worker < num_workers; ++worker) {
		string predicate;
		if (worker) {
			predicate = StringUtil::Format("lineitem.l_orderkey >= %lld", static_cast<long long>(cutoffs[worker - 1]));
		}
		if (worker + 1 < num_workers) {
			if (!predicate.empty()) {
				predicate += " AND ";
			}
			if (worker == 0 && has_nulls) {
				predicate = StringUtil::Format("(lineitem.l_orderkey < %lld OR lineitem.l_orderkey IS NULL)",
				                               static_cast<long long>(cutoffs[worker]));
			} else {
				predicate += StringUtil::Format("lineitem.l_orderkey < %lld", static_cast<long long>(cutoffs[worker]));
			}
		}
		DistributedPipelineTask task;
		task.task_id = worker;
		task.total_tasks = num_workers;
		task.task_sql = PartitionSQLGenerator::InjectWhereClause(sql, predicate);
		tasks.push_back(std::move(task));
	}
	return tasks;
}

vector<DistributedPipelineTask> TaskPartitioner::ExtractPipelineTasks(LogicalOperator &logical_plan,
                                                                      const string &base_sql, idx_t num_workers) {
	if (num_workers == 0) {
		return {};
	}

	// Only partition direct DuckDB table scans; delegate other supported queries intact.
	auto statements = conn.ExtractStatements(base_sql);
	if (statements.size() != 1 || statements[0]->type != StatementType::SELECT_STATEMENT) {
		return {};
	}
	auto &statement = statements[0]->Cast<SelectStatement>();
	if (statement.node->type != QueryNodeType::SELECT_NODE) {
		return CreateSingleTask(base_sql);
	}
	auto &select = statement.node->Cast<SelectNode>();
	if (!select.from_table || !select.cte_map.map.empty() || !select.modifiers.empty() || select.sample ||
	    select.from_table->sample || !select.from_table->column_name_alias.empty()) {
		return CreateSingleTask(base_sql);
	}
	BaseTableRef *partition_ref = nullptr;
	QueryPlanAnalyzer::RowGroupPartitionInfo row_group_info;
	if (select.from_table->type == TableReferenceType::BASE_TABLE) {
		auto *op = &logical_plan;
		while (op->children.size() == 1) {
			op = op->children[0].get();
		}
		if (op->type != LogicalOperatorType::LOGICAL_GET) {
			return CreateSingleTask(base_sql);
		}
		auto table = op->Cast<LogicalGet>().GetTable();
		auto &ref = select.from_table->Cast<BaseTableRef>();
		if (!table || !table->IsDuckTable() || ref.at_clause || !StringUtil::CIEquals(ref.table_name, table->name)) {
			return CreateSingleTask(base_sql);
		}
		ref.catalog_name = table->catalog.GetName();
		ref.schema_name = table->schema.name;
		partition_ref = &ref;
		row_group_info = analyzer.ExtractRowGroupInfo(op->Cast<LogicalGet>());
	} else if (select.from_table->type == TableReferenceType::JOIN) {
		// Every joined row belongs to exactly one row group of the selected input.
		// Restrict this first path to two native tables and a simple inner join.
		auto &join = select.from_table->Cast<JoinRef>();
		if (join.type != JoinType::INNER || join.left->type != TableReferenceType::BASE_TABLE ||
		    join.right->type != TableReferenceType::BASE_TABLE) {
			return CreateSingleTask(base_sql);
		}
		auto &left = join.left->Cast<BaseTableRef>();
		auto &right = join.right->Cast<BaseTableRef>();
		if (left.sample || right.sample || left.at_clause || right.at_clause ||
		    !left.column_name_alias.empty() || !right.column_name_alias.empty() ||
		    StringUtil::CIEquals(left.table_name, right.table_name)) {
			return CreateSingleTask(base_sql);
		}
		vector<LogicalGet *> scans;
		std::function<void(LogicalOperator &)> collect = [&](LogicalOperator &op) {
			if (op.type == LogicalOperatorType::LOGICAL_GET) {
				scans.push_back(&op.Cast<LogicalGet>());
			}
			for (auto &child : op.children) {
				collect(*child);
			}
		};
		collect(logical_plan);
		if (scans.size() != 2) {
			return CreateSingleTask(base_sql);
		}
		LogicalGet *left_scan = nullptr;
		LogicalGet *right_scan = nullptr;
		for (auto *scan : scans) {
			auto table = scan->GetTable();
			if (!table || !table->IsDuckTable()) {
				return CreateSingleTask(base_sql);
			}
			if (StringUtil::CIEquals(table->name, left.table_name)) {
				left_scan = scan;
				left.catalog_name = table->catalog.GetName();
				left.schema_name = table->schema.name;
			} else if (StringUtil::CIEquals(table->name, right.table_name)) {
				right_scan = scan;
				right.catalog_name = table->catalog.GetName();
				right.schema_name = table->schema.name;
			} else {
				return CreateSingleTask(base_sql);
			}
		}
		if (!left_scan || !right_scan) {
			return CreateSingleTask(base_sql);
		}
		auto left_groups = analyzer.ExtractRowGroupInfo(*left_scan);
		auto right_groups = analyzer.ExtractRowGroupInfo(*right_scan);
		if (!left_groups.valid || !right_groups.valid) {
			return CreateSingleTask(base_sql);
		}
		if (left_groups.total_row_groups >= right_groups.total_row_groups) {
			partition_ref = &left;
			row_group_info = std::move(left_groups);
		} else {
			partition_ref = &right;
			row_group_info = std::move(right_groups);
		}
		if (select.where_clause) {
			auto &dimension = partition_ref == &left ? right : left;
			auto &fact = partition_ref == &left ? left : right;
			auto dimension_table = (partition_ref == &left ? right_scan : left_scan)->GetTable();
			auto fact_table = (partition_ref == &left ? left_scan : right_scan)->GetTable();
			auto implied = ImpliedEqualityFilter(*select.where_clause, dimension, *dimension_table, fact, *fact_table);
			if (implied) {
				select.where_clause = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                       std::move(select.where_clause), std::move(implied));
			}
		}
	} else {
		return CreateSingleTask(base_sql);
	}
	const string task_sql = statement.ToString();

	// If reliable rowid bounds are unavailable, delegate instead of using modulo-based partitioning.
	if (!row_group_info.valid || row_group_info.total_row_groups == 0) {
		return CreateSingleTask(task_sql);
	}
	// Give each worker one contiguous run of whole row groups. Rowid pruning is row-group granular, so splitting a
	// row group makes every task read all of it; with static assignment, extra tasks per worker only break locality.
	// Tables with fewer row groups than workers leave the remaining workers idle.
	const idx_t num_tasks = std::min(num_workers, row_group_info.total_row_groups);
	const idx_t groups_per_task = row_group_info.total_row_groups / num_tasks;
	const idx_t remainder = row_group_info.total_row_groups % num_tasks;
	const string rowid = ColumnRefExpression(
	                         "rowid", partition_ref->alias.empty() ? partition_ref->table_name : partition_ref->alias)
	                         .ToString();
	vector<DistributedPipelineTask> tasks;
	tasks.reserve(num_tasks);
	for (idx_t task_idx = 0; task_idx < num_tasks; ++task_idx) {
		// Calculate which row groups this task processes; the first `remainder` tasks take one extra.
		const idx_t rg_start = task_idx * groups_per_task + std::min(task_idx, remainder);
		const idx_t rg_end = rg_start + groups_per_task + (task_idx < remainder ? 1 : 0);
		// Use actual storage starts instead of multiplying by an approximate row-group size.
		const idx_t row_start = row_group_info.row_group_starts[rg_start];
		const idx_t row_end = rg_end == row_group_info.total_row_groups ? row_group_info.rowid_end
		                                                                : row_group_info.row_group_starts[rg_end];
		DistributedPipelineTask task;
		task.task_id = task_idx;
		task.total_tasks = num_tasks;
		task.row_group_start = rg_start;
		task.row_group_end = rg_end - 1;
		// Create SQL with row group-aligned half-open rowid filter.
		task.task_sql = PartitionSQLGenerator::InjectWhereClause(
		    task_sql, StringUtil::Format("%s >= %llu AND %s < %llu", rowid, row_start, rowid, row_end));
		tasks.emplace_back(std::move(task));
	}
	return tasks;
}

} // namespace duckdb
