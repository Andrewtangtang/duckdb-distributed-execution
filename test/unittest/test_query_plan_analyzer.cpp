#include "catch/catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/partition_sql_generator.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include "server/driver/query_utils.hpp"
#include "server/driver/task_partitioner.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>

using namespace duckdb; // NOLINT

TEST_CASE("ContainsTableScan Tests", "[query_utils]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("Simple SELECT with TABLE_SCAN") {
		con.Query("CREATE TABLE test_table (id INTEGER, value VARCHAR)");
		con.Query("INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c')");

		auto result = con.Query("EXPLAIN SELECT * FROM test_table");
		REQUIRE(!result->HasError());

		auto plan = con.ExtractPlan("SELECT * FROM test_table");
		REQUIRE(plan != nullptr);

		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));

		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}

	SECTION("SELECT with GROUP BY (TABLE_SCAN as child)") {
		con.Query("CREATE TABLE group_test (category VARCHAR, value INTEGER)");
		con.Query("INSERT INTO group_test VALUES ('A', 10), ('B', 20), ('A', 30)");

		auto plan = con.ExtractPlan("SELECT category, SUM(value) FROM group_test GROUP BY category");
		REQUIRE(plan != nullptr);

		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));

		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}

	SECTION("SELECT with WHERE clause") {
		con.Query("CREATE TABLE filter_test (id INTEGER, status VARCHAR)");
		con.Query("INSERT INTO filter_test VALUES (1, 'active'), (2, 'inactive')");

		auto plan = con.ExtractPlan("SELECT * FROM filter_test WHERE status = 'active'");
		REQUIRE(plan != nullptr);

		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));

		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}

	SECTION("SELECT with multiple aggregates") {
		con.Query("CREATE TABLE agg_test (category VARCHAR, amount INTEGER, quantity INTEGER)");
		con.Query("INSERT INTO agg_test VALUES ('X', 100, 5), ('Y', 200, 10)");

		auto plan =
		    con.ExtractPlan("SELECT category, COUNT(*), SUM(amount), AVG(quantity) FROM agg_test GROUP BY category");
		REQUIRE(plan != nullptr);

		PhysicalPlanGenerator generator(*con.context);
		auto physical_plan = generator.Plan(std::move(plan));

		bool contains_scan = ContainsTableScan(physical_plan->Root());
		REQUIRE(contains_scan == true);
	}
}

TEST_CASE("Partition tasks cover deleted rowid gaps without changing WHERE precedence", "[task_partitioner]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE partition_test AS SELECT i AS id FROM range(250000) t(i)")->HasError());
	REQUIRE_FALSE(con.Query("DELETE FROM partition_test WHERE id >= 10000 AND id < 20000")->HasError());

	QueryPlanAnalyzer analyzer(con);
	TaskPartitioner partitioner(con, analyzer);
	const string limit_sql = "SELECT id FROM partition_test LIMIT 10";
	auto limit_plan = con.ExtractPlan(limit_sql);
	REQUIRE(limit_plan != nullptr);
	REQUIRE(partitioner.ExtractPipelineTasks(*limit_plan, limit_sql, 3).size() == 1);
	for (const auto workers : {3, 5}) {
		const string sql = "SELECT t.id FROM partition_test AS t WHERE t.id = 5 OR t.id = 200005";
		auto plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		auto tasks = partitioner.ExtractPipelineTasks(*plan, sql, workers);
		// Never split a row group, so extra workers get no task.
		const auto total_row_groups = analyzer.ExtractRowGroupInfo(*plan).total_row_groups;
		REQUIRE(total_row_groups > 1);
		REQUIRE(tasks.size() == std::min<idx_t>(workers, total_row_groups));
		idx_t matches = 0;
		for (const auto &task : tasks) {
			auto result = con.Query(StringUtil::Format("SELECT count(*) FROM (%s)", task.task_sql));
			REQUIRE_FALSE(result->HasError());
			matches += result->GetValue(0, 0).GetValue<idx_t>();
		}
		REQUIRE(matches == 2);

		const string full_sql = "SELECT id FROM partition_test";
		plan = con.ExtractPlan(full_sql);
		REQUIRE(plan != nullptr);
		tasks = partitioner.ExtractPipelineTasks(*plan, full_sql, workers);
		idx_t rows = 0;
		for (const auto &task : tasks) {
			auto result = con.Query(StringUtil::Format("SELECT count(*) FROM (%s)", task.task_sql));
			REQUIRE_FALSE(result->HasError());
			rows += result->GetValue(0, 0).GetValue<idx_t>();
		}
		REQUIRE(rows == 240000);
	}
}

TEST_CASE("Local ObjFS scans assign contiguous whole row groups", "[task_partitioner]") {
	auto root = std::filesystem::temp_directory_path() /
	            StringUtil::Format("duckherder_partition_%s",
	                               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	std::filesystem::create_directories(root);
	{
		DuckDB writer_db(nullptr);
		Connection writer(writer_db);
		REQUIRE_FALSE(writer.Query("LOAD duckdb_object_storage")->HasError());
		REQUIRE_FALSE(writer.Query("SET duckdb_objfs_backend = 'local'")->HasError());
		REQUIRE_FALSE(writer.Query(StringUtil::Format("SET duckdb_objfs_root = '%s'", root.string()))->HasError());
		// Small row groups give a multi-row-group table without writing 122,880 rows per group.
		REQUIRE_FALSE(
		    writer.Query("ATTACH 'duckdb_objfs://partition.db' AS object_db (ROW_GROUP_SIZE 2048)")->HasError());
		REQUIRE_FALSE(writer.Query("CREATE TABLE object_db.t AS SELECT i AS id FROM range(10000) t(i)")->HasError());
		REQUIRE_FALSE(writer.Query("CREATE TABLE object_db.tiny AS SELECT i AS id FROM range(8) t(i)")->HasError());
	}
	{
		DuckDB reader_db(nullptr);
		Connection reader(reader_db);
		REQUIRE_FALSE(reader.Query("LOAD duckdb_object_storage")->HasError());
		REQUIRE_FALSE(reader.Query("SET duckdb_objfs_backend = 'local'")->HasError());
		REQUIRE_FALSE(reader.Query(StringUtil::Format("SET duckdb_objfs_root = '%s'", root.string()))->HasError());
		REQUIRE_FALSE(reader.Query("ATTACH 'duckdb_objfs://partition.db' AS object_db (READ_ONLY)")->HasError());
		QueryPlanAnalyzer analyzer(reader);
		TaskPartitioner partitioner(reader, analyzer);

		// A single row group is never split across workers.
		const string tiny_sql = "SELECT id FROM object_db.tiny";
		auto plan = reader.ExtractPlan(tiny_sql);
		REQUIRE(plan != nullptr);
		REQUIRE(partitioner.ExtractPipelineTasks(*plan, tiny_sql, 3).size() == 1);

		const string sql = "SELECT id FROM object_db.t";
		plan = reader.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		auto row_group_info = analyzer.ExtractRowGroupInfo(*plan);
		REQUIRE(row_group_info.total_row_groups >= 3);
		auto tasks = partitioner.ExtractPipelineTasks(*plan, sql, 3);
		REQUIRE(tasks.size() == 3);
		idx_t rows = 0;
		idx_t next_row_group = 0;
		for (const auto &task : tasks) {
			// Each task owns the next contiguous run of row groups.
			REQUIRE(task.row_group_start == next_row_group);
			REQUIRE(task.row_group_end >= task.row_group_start);
			next_row_group = task.row_group_end + 1;
			auto result = reader.Query(StringUtil::Format("SELECT count(*) FROM (%s)", task.task_sql));
			REQUIRE_FALSE(result->HasError());
			rows += result->GetValue(0, 0).GetValue<idx_t>();
		}
		REQUIRE(next_row_group == row_group_info.total_row_groups);
		REQUIRE(rows == 10000);
	}
	std::filesystem::remove_all(root);
}

TEST_CASE("Partial aggregate SQL preserves global aggregate semantics", "[partial_aggregate]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE aggregates(category VARCHAR, value INTEGER, keep BOOLEAN)")->HasError());
	REQUIRE_FALSE(con.Query("INSERT INTO aggregates VALUES ('a', 1, true), ('a', NULL, true), ('a', 9, true), "
	                        "('b', -2, true), ('b', 20, true), ('c', 100, false)")
	                  ->HasError());

	auto run_partial = [&](const string &sql) {
		auto plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		QueryPlanAnalyzer analyzer(con);
		auto analysis = analyzer.AnalyzeQuery(*plan, sql);
		REQUIRE(analysis.supports_partitioned_aggregation);
		REQUIRE_FALSE(con.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", QueryPlanAnalyzer::PARTIAL_TABLE_NAME))
		                  ->HasError());
		REQUIRE_FALSE(con.Query(StringUtil::Format("CREATE TEMP TABLE %s AS SELECT * FROM (%s) partials "
		                                           "WHERE false",
		                                           QueryPlanAnalyzer::PARTIAL_TABLE_NAME, analysis.partial_sql))
		                  ->HasError());
		for (const auto &predicate : {"rowid < 3", "rowid >= 3"}) {
			auto task_sql = PartitionSQLGenerator::InjectWhereClause(analysis.partial_sql, predicate);
			REQUIRE_FALSE(
			    con.Query(StringUtil::Format("INSERT INTO %s %s", QueryPlanAnalyzer::PARTIAL_TABLE_NAME, task_sql))
			        ->HasError());
		}
		return con.Query(analysis.final_sql);
	};
	for (const auto &sql :
	     {"SELECT SUM(value) FROM aggregates", "SELECT category AS key, SUM(value) FROM aggregates GROUP BY 1",
	      "SELECT SUM(value) FILTER (WHERE value > 0) FROM aggregates",
	      "SELECT SUM(value) FROM aggregates HAVING AVG(value) >= 5",
	      "SELECT SUM(value) AS total FROM aggregates ORDER BY total LIMIT 1 OFFSET 0"}) {
		INFO(sql);
		auto plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		QueryPlanAnalyzer analyzer(con);
		REQUIRE(analyzer.AnalyzeQuery(*plan, sql).supports_partitioned_aggregation);
	}

	const string grouped_sql =
	    "SELECT category AS key, SUM(value) FILTER (WHERE value > 0) AS total, COUNT(*) AS n, MIN(value) AS lo, "
	    "MAX(value) AS hi, AVG(value) AS mean FROM aggregates WHERE keep GROUP BY 1 HAVING AVG(value) >= 5 "
	    "ORDER BY total DESC LIMIT 1 OFFSET 0";
	auto expected = con.Query(grouped_sql);
	auto actual = run_partial(grouped_sql);
	REQUIRE_FALSE(expected->HasError());
	REQUIRE_FALSE(actual->HasError());
	REQUIRE(actual->RowCount() == expected->RowCount());
	for (idx_t col = 0; col < expected->ColumnCount(); ++col) {
		REQUIRE(actual->GetValue(col, 0) == expected->GetValue(col, 0));
	}

	const string empty_sql =
	    "SELECT SUM(value), COUNT(value), MIN(value), MAX(value), AVG(value) FROM aggregates WHERE false";
	expected = con.Query(empty_sql);
	actual = run_partial(empty_sql);
	REQUIRE_FALSE(actual->HasError());
	for (idx_t col = 0; col < expected->ColumnCount(); ++col) {
		auto expected_value = expected->GetValue(col, 0);
		auto actual_value = actual->GetValue(col, 0);
		REQUIRE(actual_value.IsNull() == expected_value.IsNull());
		if (!expected_value.IsNull()) {
			REQUIRE(actual_value == expected_value);
		}
	}
}

TEST_CASE("Unsupported aggregates retain fallback", "[partial_aggregate]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE aggregates(category INTEGER, value INTEGER)")->HasError());
	QueryPlanAnalyzer analyzer(con);
	for (const auto &sql : {"SELECT MEDIAN(value) FROM aggregates", "SELECT SUM(DISTINCT value) FROM aggregates",
	                        "SELECT SUM(value), MEDIAN(value) FROM aggregates",
	                        "SELECT category, SUM(value) FROM aggregates GROUP BY GROUPING SETS ((category), ())",
	                        "SELECT value % 2, SUM(value) FROM aggregates GROUP BY value % 2",
	                        "SELECT SUM((SELECT value)) FROM aggregates"}) {
		auto plan = con.ExtractPlan(sql);
		REQUIRE(plan != nullptr);
		REQUIRE_FALSE(analyzer.AnalyzeQuery(*plan, sql).supports_partitioned_aggregation);
	}
}
