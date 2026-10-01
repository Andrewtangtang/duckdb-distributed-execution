#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/types.hpp"
#include "server/driver/query_plan_analyzer.hpp"
#include <arrow/flight/client.h>
#include <memory>

namespace duckdb {

// ResultMerger: Collects and merges results from distributed workers.
// Applies intelligent merge strategies based on query type.
class ResultMerger {
public:
	explicit ResultMerger(Connection &conn_p);

	// Collect and merge results from worker streams (simple concatenation).
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &names, const vector<LogicalType> &types);

	// Collect and merge results with smart merging based on query analysis.
	unique_ptr<QueryResult> CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                                               const vector<string> &partial_names,
	                                               const vector<LogicalType> &partial_types,
	                                               const vector<string> &output_names,
	                                               const vector<LogicalType> &output_types,
	                                               const QueryPlanAnalyzer::QueryAnalysis &query_analysis);

	// Concatenate worker result batches without decoding them.
	// Returns an invalid status if a worker batch does not match the schema the driver would produce for this result.
	arrow::Status CollectResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
	                             const vector<string> &names, const vector<LogicalType> &types,
	                             std::shared_ptr<arrow::Schema> &schema,
	                             vector<std::shared_ptr<arrow::RecordBatch>> &batches);

private:
	Connection &conn;
};

} // namespace duckdb
