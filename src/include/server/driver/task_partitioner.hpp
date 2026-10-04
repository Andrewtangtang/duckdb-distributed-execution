#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

// Forward declarations.
struct DistributedPipelineTask;
class QueryPlanAnalyzer;

// Creates distributed tasks from logical plans.
// Assigns each worker a contiguous run of whole row groups when storage bounds are available.
class TaskPartitioner {
public:
	TaskPartitioner(Connection &conn, QueryPlanAnalyzer &analyzer);

	// Extract distributed pipeline tasks from logical plan.
	// Creates tasks aligned with native DuckDB row groups when possible.
	vector<DistributedPipelineTask> ExtractPipelineTasks(LogicalOperator &logical_plan, const string &base_sql,
	                                                     idx_t num_workers);

	// Q3: use the ordered lineitem.l_orderkey row-group statistics to assign complete keys to workers.
	// Returns no tasks when the data is not ordered or statistics are unavailable.
	vector<DistributedPipelineTask> ExtractOrderKeyRangeTasks(const string &sql, idx_t num_workers);

private:
	// Create a single non-distributed task.
	vector<DistributedPipelineTask> CreateSingleTask(const string &base_sql);

	Connection &conn;
	QueryPlanAnalyzer &analyzer;
};

} // namespace duckdb
