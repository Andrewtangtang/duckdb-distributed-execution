#include "server/driver/query_plan_analyzer.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

namespace {

enum class PartialAggregateType { UNSUPPORTED, COUNT, SUM, MIN, MAX, AVG };

struct PartialAggregate {
	PartialAggregateType type;
	unique_ptr<ParsedExpression> expression;
};

PartialAggregateType GetAggregateType(const string &function_name) {
	const auto name = StringUtil::Lower(function_name);
	if (name == "count" || name == "count_star") {
		return PartialAggregateType::COUNT;
	}
	if (name == "sum" || name == "sum_no_overflow") {
		return PartialAggregateType::SUM;
	}
	if (name == "min") {
		return PartialAggregateType::MIN;
	}
	if (name == "max") {
		return PartialAggregateType::MAX;
	}
	if (name == "avg") {
		return PartialAggregateType::AVG;
	}
	return PartialAggregateType::UNSUPPORTED;
}

unique_ptr<ParsedExpression> ParseExpression(const string &sql) {
	auto expressions = Parser::ParseExpressionList(sql);
	D_ASSERT(expressions.size() == 1);
	return std::move(expressions[0]);
}

unique_ptr<ParsedExpression> WithoutAlias(const ParsedExpression &expression) {
	auto result = expression.Copy();
	result->ClearAlias();
	return result;
}

bool SameExpression(const ParsedExpression &left, const ParsedExpression &right) {
	auto left_copy = WithoutAlias(left);
	auto right_copy = WithoutAlias(right);
	return left_copy->Equals(*right_copy);
}

optional_idx FindGroup(const ParsedExpression &expression, const vector<unique_ptr<ParsedExpression>> &groups,
                       const vector<string> &aliases) {
	for (idx_t idx = 0; idx < groups.size(); ++idx) {
		if (SameExpression(expression, *groups[idx])) {
			return optional_idx(idx);
		}
	}
	if (expression.expression_class == ExpressionClass::COLUMN_REF) {
		const auto &column = expression.Cast<ColumnRefExpression>();
		if (column.column_names.size() == 1) {
			for (idx_t idx = 0; idx < aliases.size(); ++idx) {
				if (!aliases[idx].empty() && StringUtil::CIEquals(column.column_names[0], aliases[idx])) {
					return optional_idx(idx);
				}
			}
		}
	}
	return optional_idx();
}

idx_t FindOrAddAggregate(const FunctionExpression &function, vector<PartialAggregate> &aggregates) {
	auto expression = WithoutAlias(function);
	for (idx_t idx = 0; idx < aggregates.size(); ++idx) {
		if (expression->Equals(*aggregates[idx].expression)) {
			return idx;
		}
	}
	auto type = GetAggregateType(function.function_name);
	D_ASSERT(type != PartialAggregateType::UNSUPPORTED);
	aggregates.push_back({type, std::move(expression)});
	return aggregates.size() - 1;
}

void CollectAggregates(const ParsedExpression &expression, vector<PartialAggregate> &aggregates) {
	if (expression.expression_class == ExpressionClass::FUNCTION) {
		const auto &function = expression.Cast<FunctionExpression>();
		if (GetAggregateType(function.function_name) != PartialAggregateType::UNSUPPORTED) {
			FindOrAddAggregate(function, aggregates);
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expression, [&](const ParsedExpression &child) { CollectAggregates(child, aggregates); });
}

bool ValidateParsedAggregates(const ParsedExpression &expression) {
	if (expression.expression_class == ExpressionClass::FUNCTION) {
		const auto &function = expression.Cast<FunctionExpression>();
		if (GetAggregateType(function.function_name) != PartialAggregateType::UNSUPPORTED) {
			if (function.distinct || function.export_state ||
			    (function.order_bys && !function.order_bys->orders.empty())) {
				return false;
			}
			for (const auto &child : function.children) {
				if (child->HasSubquery() || child->IsWindow()) {
					return false;
				}
			}
			return !function.filter || (!function.filter->HasSubquery() && !function.filter->IsWindow());
		}
	}
	bool valid = true;
	ParsedExpressionIterator::EnumerateChildren(
	    expression, [&](const ParsedExpression &child) { valid &= ValidateParsedAggregates(child); });
	return valid;
}

void RewriteFinalExpression(unique_ptr<ParsedExpression> &expression,
                            const vector<unique_ptr<ParsedExpression>> &groups, const vector<string> &group_aliases,
                            vector<PartialAggregate> &aggregates) {
	auto alias = expression->GetAlias();
	auto group_idx = FindGroup(*expression, groups, group_aliases);
	if (group_idx.IsValid()) {
		expression = make_uniq<ColumnRefExpression>(StringUtil::Format("__g%llu", group_idx.GetIndex()));
		expression->SetAlias(std::move(alias));
		return;
	}
	if (expression->expression_class == ExpressionClass::FUNCTION) {
		auto &function = expression->Cast<FunctionExpression>();
		if (GetAggregateType(function.function_name) != PartialAggregateType::UNSUPPORTED) {
			auto aggregate_idx = FindOrAddAggregate(function, aggregates);
			auto column = StringUtil::Format("__a%llu", static_cast<long long unsigned>(aggregate_idx));
			string sql;
			switch (aggregates[aggregate_idx].type) {
			case PartialAggregateType::UNSUPPORTED:
				throw InternalException("Cannot rewrite an unsupported partial aggregate");
			case PartialAggregateType::COUNT:
			case PartialAggregateType::SUM:
				sql = StringUtil::Format("sum(%s_v)", column);
				break;
			case PartialAggregateType::MIN:
				sql = StringUtil::Format("min(%s_v)", column);
				break;
			case PartialAggregateType::MAX:
				sql = StringUtil::Format("max(%s_v)", column);
				break;
			case PartialAggregateType::AVG:
				sql = StringUtil::Format("sum(%s_s) / sum(%s_c)", column, column);
				break;
			}
			expression = ParseExpression(sql);
			expression->SetAlias(std::move(alias));
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(*expression, [&](unique_ptr<ParsedExpression> &child) {
		RewriteFinalExpression(child, groups, group_aliases, aggregates);
	});
}

bool ValidateBoundAggregates(LogicalOperator &logical_plan) {
	bool valid = true;
	std::function<void(LogicalOperator &)> visit = [&](LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			for (const auto &expression : op.Cast<LogicalAggregate>().expressions) {
				const auto &aggregate = expression->Cast<BoundAggregateExpression>();
				const auto type = GetAggregateType(aggregate.function.name);
				valid &= type != PartialAggregateType::UNSUPPORTED && !aggregate.IsDistinct() &&
				         (!aggregate.order_bys || aggregate.order_bys->orders.empty());
				if (type == PartialAggregateType::AVG) {
					valid &= aggregate.children.size() == 1 && aggregate.children[0]->return_type.IsNumeric();
				}
			}
		}
		for (auto &child : op.children) {
			visit(*child);
		}
	};
	visit(logical_plan);
	return valid;
}

bool AnalyzeAggregateOutputs(const string &sql, QueryPlanAnalyzer::QueryAnalysis &analysis) {
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return false;
	}
	auto &statement = parser.statements[0]->Cast<SelectStatement>();
	if (statement.node->type != QueryNodeType::SELECT_NODE) {
		return false;
	}
	auto &select = statement.node->Cast<SelectNode>();
	if (!select.from_table || select.from_table->type != TableReferenceType::BASE_TABLE ||
	    !select.cte_map.map.empty() || select.sample || select.from_table->sample || select.qualify ||
	    select.groups.grouping_sets.size() > 1) {
		return false;
	}
	if (!select.groups.grouping_sets.empty() &&
	    select.groups.grouping_sets[0].size() != select.groups.group_expressions.size()) {
		return false;
	}

	vector<unique_ptr<ParsedExpression>> groups;
	vector<string> group_aliases;
	for (const auto &group : select.groups.group_expressions) {
		unique_ptr<ParsedExpression> resolved;
		if (group->expression_class == ExpressionClass::CONSTANT) {
			const auto &value = group->Cast<ConstantExpression>().value;
			if (!value.type().IsIntegral()) {
				return false;
			}
			auto ordinal = value.GetValue<int64_t>();
			if (ordinal <= 0 || NumericCast<idx_t>(ordinal) > select.select_list.size()) {
				return false;
			}
			resolved = WithoutAlias(*select.select_list[NumericCast<idx_t>(ordinal) - 1]);
		} else if (group->expression_class == ExpressionClass::COLUMN_REF &&
		           group->Cast<ColumnRefExpression>().column_names.size() == 1) {
			const auto &name = group->Cast<ColumnRefExpression>().column_names[0];
			for (const auto &output : select.select_list) {
				if (!output->GetAlias().empty() && StringUtil::CIEquals(output->GetAlias(), name)) {
					resolved = WithoutAlias(*output);
					break;
				}
			}
		}
		if (!resolved) {
			resolved = WithoutAlias(*group);
		}
		// Workers output each group as `__g<idx>`, so any per-row expression can be merged on the driver.
		if (resolved->HasSubquery() || resolved->IsWindow()) {
			return false;
		}
		groups.push_back(std::move(resolved));
		group_aliases.emplace_back();
	}
	for (const auto &output : select.select_list) {
		if (output->expression_class == ExpressionClass::FUNCTION &&
		    GetAggregateType(output->Cast<FunctionExpression>().function_name) != PartialAggregateType::UNSUPPORTED) {
			continue;
		}
		auto group_idx = FindGroup(*output, groups, group_aliases);
		if (!group_idx.IsValid()) {
			return false;
		}
		group_aliases[group_idx.GetIndex()] = output->GetAlias();
	}

	vector<PartialAggregate> aggregates;
	for (const auto &output : select.select_list) {
		if (!ValidateParsedAggregates(*output)) {
			return false;
		}
		CollectAggregates(*output, aggregates);
	}
	if (select.having) {
		if (!ValidateParsedAggregates(*select.having)) {
			return false;
		}
		CollectAggregates(*select.having, aggregates);
	}
	for (const auto &modifier : select.modifiers) {
		if (modifier->type == ResultModifierType::DISTINCT_MODIFIER ||
		    modifier->type == ResultModifierType::LIMIT_PERCENT_MODIFIER) {
			return false;
		}
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			for (const auto &order : modifier->Cast<OrderModifier>().orders) {
				if (!ValidateParsedAggregates(*order.expression)) {
					return false;
				}
				CollectAggregates(*order.expression, aggregates);
			}
		}
	}
	if (aggregates.empty()) {
		return false;
	}

	auto partial_copy = statement.Copy();
	auto &partial_statement = partial_copy->Cast<SelectStatement>();
	auto &partial = partial_statement.node->Cast<SelectNode>();
	partial.select_list.clear();
	partial.groups.group_expressions.clear();
	partial.groups.grouping_sets.clear();
	GroupingSet grouping_set;
	for (idx_t idx = 0; idx < groups.size(); ++idx) {
		auto group = groups[idx]->Copy();
		group->SetAlias(StringUtil::Format("__g%llu", static_cast<long long unsigned>(idx)));
		partial.select_list.push_back(std::move(group));
		partial.groups.group_expressions.push_back(groups[idx]->Copy());
		grouping_set.insert(idx);
	}
	if (!groups.empty()) {
		partial.groups.grouping_sets.push_back(std::move(grouping_set));
	}
	for (idx_t idx = 0; idx < aggregates.size(); ++idx) {
		const auto &aggregate = aggregates[idx];
		auto function = aggregate.expression->Copy();
		auto prefix = StringUtil::Format("__a%llu", static_cast<long long unsigned>(idx));
		if (aggregate.type == PartialAggregateType::AVG) {
			function->Cast<FunctionExpression>().function_name = "sum";
			function->SetAlias(StringUtil::Format("%s_s", prefix));
			partial.select_list.push_back(std::move(function));
			auto count = aggregate.expression->Copy();
			count->Cast<FunctionExpression>().function_name = "count";
			count->SetAlias(StringUtil::Format("%s_c", prefix));
			partial.select_list.push_back(std::move(count));
		} else {
			function->SetAlias(StringUtil::Format("%s_v", prefix));
			partial.select_list.push_back(std::move(function));
		}
	}
	partial.having.reset();
	partial.modifiers.clear();
	analysis.partial_sql = partial_statement.ToString();

	auto final_copy = statement.Copy();
	auto &final_statement = final_copy->Cast<SelectStatement>();
	auto &final = final_statement.node->Cast<SelectNode>();
	for (auto &output : final.select_list) {
		RewriteFinalExpression(output, groups, group_aliases, aggregates);
	}
	if (final.having) {
		RewriteFinalExpression(final.having, groups, group_aliases, aggregates);
	}
	for (auto &modifier : final.modifiers) {
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			for (auto &order : modifier->Cast<OrderModifier>().orders) {
				RewriteFinalExpression(order.expression, groups, group_aliases, aggregates);
			}
		}
	}
	auto partial_table = make_uniq<BaseTableRef>();
	partial_table->table_name = QueryPlanAnalyzer::PARTIAL_TABLE_NAME;
	final.from_table = std::move(partial_table);
	final.where_clause.reset();
	final.groups.group_expressions.clear();
	final.groups.grouping_sets.clear();
	GroupingSet final_grouping_set;
	for (idx_t idx = 0; idx < groups.size(); ++idx) {
		final.groups.group_expressions.push_back(
		    make_uniq<ColumnRefExpression>(StringUtil::Format("__g%llu", static_cast<long long unsigned>(idx))));
		final_grouping_set.insert(idx);
	}
	if (!groups.empty()) {
		final.groups.grouping_sets.push_back(std::move(final_grouping_set));
	}
	analysis.final_sql = final_statement.ToString();
	return true;
}

} // namespace

QueryPlanAnalyzer::QueryPlanAnalyzer(Connection &conn_p) : conn(conn_p) {
}

idx_t QueryPlanAnalyzer::QueryEstimatedParallelism(LogicalOperator &logical_plan) {
	idx_t estimated_threads = 0;

	// Wrap physical plan generation in a transaction (mimicking DuckDB's internal behavior).
	// This is necessary because physical plan generation requires an active transaction context.
	conn.context->RunFunctionInTransaction([&]() {
		auto cloned_logical_plan = logical_plan.Copy(*conn.context);
		PhysicalPlanGenerator generator(*conn.context);
		auto physical_plan = generator.Plan(std::move(cloned_logical_plan));

		// Query the estimated thread count.
		// This tells us how many parallel tasks DuckDB would naturally create.
		estimated_threads = physical_plan->Root().EstimatedThreadCount();
	});

	return estimated_threads;
}

QueryPlanAnalyzer::RowGroupPartitionInfo QueryPlanAnalyzer::ExtractRowGroupInfo(LogicalOperator &logical_plan) {
	RowGroupPartitionInfo row_group_info;

	conn.context->RunFunctionInTransaction([&]() {
		// Find the native table scan instead of generating a physical plan to estimate row groups.
		auto *op = &logical_plan;
		while (op->children.size() == 1) {
			op = op->children[0].get();
		}
		if (op->type != LogicalOperatorType::LOGICAL_GET) {
			return;
		}
		auto table = op->Cast<LogicalGet>().GetTable();
		if (!table || !table->IsDuckTable() || table->ColumnExists("rowid")) {
			return;
		}
		// Use actual storage boundaries instead of estimated cardinality and DEFAULT_ROW_GROUP_SIZE
		// to obtain row groups.
		auto &storage = table->GetStorage();
		auto row_groups = storage.GetRowGroupCollection()->GetRowGroups();
		for (auto segment = row_groups->GetRootSegment(); segment; segment = row_groups->GetNextSegment(*segment)) {
			row_group_info.row_group_starts.push_back(segment->GetRowStart());
			row_group_info.rowid_end = segment->GetRowEnd();
		}
		// Calculate the number of row groups from storage metadata.
		row_group_info.total_row_groups = row_group_info.row_group_starts.size();
		// Mark native storage metadata as valid, including an empty table with no row groups.
		row_group_info.valid = true;
	});

	return row_group_info;
}

QueryPlanAnalyzer::PipelineInfo QueryPlanAnalyzer::AnalyzePipelines(LogicalOperator &logical_plan) {
	PipelineInfo info;

	conn.context->RunFunctionInTransaction([&]() {
		// Generate physical plan to analyze pipelines
		auto cloned_plan = logical_plan.Copy(*conn.context);
		PhysicalPlanGenerator generator(*conn.context);
		auto physical_plan_ptr = generator.Plan(std::move(cloned_plan));
		auto &physical_plan = physical_plan_ptr->Root();

		// Recursively analyze the physical plan structure
		std::function<void(PhysicalOperator &)> analyze_operator = [&](PhysicalOperator &op) {
			auto op_type = op.type;

			// Check for operators that typically create multiple pipelines
			switch (op_type) {
			case PhysicalOperatorType::HASH_JOIN:
			case PhysicalOperatorType::NESTED_LOOP_JOIN:
			case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
			case PhysicalOperatorType::CROSS_PRODUCT:
			case PhysicalOperatorType::IE_JOIN:
			case PhysicalOperatorType::ASOF_JOIN:
				info.has_joins = true;
				info.is_simple_scan = false;
				info.pipeline_types.emplace_back("JOIN");
				break;

			case PhysicalOperatorType::WINDOW:
			case PhysicalOperatorType::STREAMING_WINDOW:
				info.has_complex_operators = true;
				info.is_simple_scan = false;
				info.pipeline_types.emplace_back("WINDOW");
				break;

			case PhysicalOperatorType::RECURSIVE_CTE:
			case PhysicalOperatorType::CTE:
				info.has_complex_operators = true;
				info.is_simple_scan = false;
				info.pipeline_types.emplace_back("CTE");
				break;

			case PhysicalOperatorType::HASH_GROUP_BY:
			case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
				info.pipeline_types.emplace_back("HASH_AGG");
				break;

			case PhysicalOperatorType::ORDER_BY:
				info.pipeline_types.emplace_back("ORDER_BY");
				break;

			case PhysicalOperatorType::TABLE_SCAN:
				info.pipeline_types.emplace_back("TABLE_SCAN");
				break;

			default:
				break;
			}

			// Recurse into children
			for (auto &child : op.children) {
				analyze_operator(child.get());
			}
		};

		analyze_operator(physical_plan);

		// Estimate pipeline count based on operators found
		// Simple heuristic: each join/window/CTE creates additional pipelines
		info.pipeline_count = 1; // Base pipeline
		if (info.has_joins) {
			info.pipeline_count += 1; // Build + probe = 2 phases
			info.has_dependencies = true;
		}
		if (info.has_complex_operators) {
			info.pipeline_count += 1;
			info.has_dependencies = true;
		}
	});

	return info;
}

QueryPlanAnalyzer::QueryAnalysis QueryPlanAnalyzer::AnalyzeQuery(LogicalOperator &logical_plan, const string &sql) {
	QueryAnalysis analysis;

	// Recursively walk the logical plan tree to find aggregates, GROUP BY, DISTINCT
	std::function<void(LogicalOperator &)> analyze_operator = [&](LogicalOperator &op) {
		// Check for AGGREGATE operator
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			analysis.has_aggregation = true;

			auto &agg_op = op.Cast<LogicalAggregate>();

			// Check if this is a GROUP BY aggregation
			if (!agg_op.groups.empty()) {
				analysis.has_group_by = true;
			}
		}

		// Check for DISTINCT operator
		if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
			analysis.has_distinct = true;
		}

		// Check for ORDER BY operator
		if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
			analysis.has_order_by = true;
		}

		// Recursively analyze children
		for (auto &child : op.children) {
			analyze_operator(*child);
		}
	};

	// Start analysis from root
	analyze_operator(logical_plan);

	// Determine merge strategy based on what we found
	if (analysis.has_group_by) {
		analysis.merge_strategy = MergeStrategy::GROUP_BY_MERGE;
	} else if (analysis.has_aggregation) {
		analysis.merge_strategy = MergeStrategy::AGGREGATE_MERGE;
	} else if (analysis.has_distinct) {
		analysis.merge_strategy = MergeStrategy::DISTINCT_MERGE;
	} else {
		analysis.merge_strategy = MergeStrategy::CONCATENATE;
	}
	if (analysis.has_aggregation) {
		analysis.supports_partitioned_aggregation =
		    ValidateBoundAggregates(logical_plan) && AnalyzeAggregateOutputs(sql, analysis);
	}

	return analysis;
}

} // namespace duckdb
