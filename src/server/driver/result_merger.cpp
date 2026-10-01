#include "server/driver/result_merger.hpp"
#include "arrow_utils.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include <arrow/c/bridge.h>
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

ResultMerger::ResultMerger(Connection &conn_p) : conn(conn_p) {
}

unique_ptr<QueryResult>
ResultMerger::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                     const vector<string> &names, const vector<LogicalType> &types) {
	// Coordinator acts as GlobalState aggregator in DuckDB's parallel execution model
	//
	// DuckDB's parallel execution pattern:
	// 1. Multiple threads execute in parallel, each with LocalSinkState
	// 2. Combine() merges LocalSinkState into GlobalSinkState
	// 3. Finalize() produces the final result from GlobalSinkState
	//
	// Distributed execution mapping:
	// 1. Multiple worker nodes execute in parallel (each = one thread)
	// 2. Each worker returns LocalState output (as Arrow RecordBatches)
	// 3. This method performs the Combine() operation:
	//    - Collects LocalState outputs from all workers
	//    - Merges them into a unified result (GlobalState)
	// 4. The ColumnDataCollection acts as our GlobalSinkState
	//
	// This maintains the same aggregation semantics as thread-level parallelism,
	// but distributed across network-connected nodes.
	//
	// Collection will be created lazily after we see the first batch's schema
	unique_ptr<ColumnDataCollection> collection;
	vector<LogicalType> actual_types; // Types from actual Arrow data

	// Combine phase: Merge LocalState outputs from each worker
	idx_t worker_idx = 0;
	idx_t total_batches = 0;
	idx_t total_rows_combined = 0;

	for (auto &stream : streams) {
		idx_t worker_batches = 0;
		idx_t worker_rows = 0;

		while (true) {
			auto batch_result = stream->Next();
			if (!batch_result.ok()) {
				throw IOException("Failed reading worker result: %s", batch_result.status().ToString());
			}

			auto batch_with_metadata = batch_result.ValueOrDie();
			if (!batch_with_metadata.data) {
				break; // End of stream from this worker
			}

			// Convert Arrow batch (LocalState output) to DuckDB DataChunk
			auto arrow_batch = batch_with_metadata.data;
			DataChunk chunk;
			ArrowRecordBatchToDataChunk(*conn.context, *arrow_batch, chunk, types.empty() ? nullptr : &types);

			// Initialize collection with actual schema from first batch
			if (!collection) {
				actual_types = chunk.GetTypes();
				collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), actual_types);
			}

			// Append to GlobalSinkState (ColumnDataCollection)
			collection->Append(chunk);

			worker_batches++;
			worker_rows += arrow_batch->num_rows();
			total_batches++;
			total_rows_combined += arrow_batch->num_rows();
		}
		worker_idx++;
	}

	// Finalize phase: Return the aggregated result
	// In this simple case, we just return the merged collection
	// For more complex operators (aggregates, sorts, etc.), additional
	// finalization logic would go here (e.g., final aggregation, final sort)
	if (!collection) {
		collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, StatementProperties {}, names,
	                                          std::move(collection), ClientProperties {});
}

unique_ptr<QueryResult>
ResultMerger::CollectAndMergeResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                     const vector<string> &partial_names, const vector<LogicalType> &partial_types,
                                     const vector<string> &output_names, const vector<LogicalType> &output_types,
                                     const QueryPlanAnalyzer::QueryAnalysis &query_analysis) {
	// Collect results from all workers
	auto partial_result = CollectAndMergeResults(streams, partial_names, partial_types);

	// For simple scans, just return the concatenated results
	if (query_analysis.merge_strategy == QueryPlanAnalyzer::MergeStrategy::CONCATENATE) {
		return partial_result;
	}

	auto materialized = dynamic_cast<MaterializedQueryResult *>(partial_result.get());
	if (!materialized) {
		return partial_result;
	}

	// Create a temporary table from the collected results
	const auto temp_table_name = QueryPlanAnalyzer::PARTIAL_TABLE_NAME;

	// Drop if exists
	conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));

	// Create table with internal column names so aliases cannot affect merge semantics.
	string create_sql = StringUtil::Format("CREATE TEMPORARY TABLE %s (", temp_table_name);
	for (idx_t idx = 0; idx < partial_names.size(); ++idx) {
		if (idx > 0) {
			create_sql += ", ";
		}
		create_sql +=
		    StringUtil::Format("%s %s", SQLIdentifier::ToString(partial_names[idx]), partial_types[idx].ToString());
	}
	create_sql += ")";

	auto create_result = conn.Query(create_sql);
	if (create_result->HasError()) {
		throw IOException("Failed to create temp table: %s", create_result->GetError());
	}

	// Append whole chunks; a per-row INSERT statement costs a full parse, bind and execute.
	Appender appender(conn, TEMP_CATALOG, DEFAULT_SCHEMA, temp_table_name);
	ColumnDataScanState scan_state;
	auto &collection = materialized->Collection();
	collection.InitializeScan(scan_state);
	DataChunk chunk;
	collection.InitializeScanChunk(chunk);
	while (collection.Scan(scan_state, chunk)) {
		appender.AppendDataChunk(chunk);
	}
	appender.Close();

	// Apply the appropriate merge strategy
	string merge_sql;

	switch (query_analysis.merge_strategy) {
	case QueryPlanAnalyzer::MergeStrategy::AGGREGATE_MERGE:
	case QueryPlanAnalyzer::MergeStrategy::GROUP_BY_MERGE:
		merge_sql = "WITH __final(";
		for (idx_t idx = 0; idx < output_names.size(); ++idx) {
			if (idx > 0) {
				merge_sql += ", ";
			}
			merge_sql += StringUtil::Format("__o%llu", static_cast<long long unsigned>(idx));
		}
		merge_sql += StringUtil::Format(") AS (%s) SELECT ", query_analysis.final_sql);
		for (idx_t idx = 0; idx < output_names.size(); ++idx) {
			if (idx > 0) {
				merge_sql += ", ";
			}
			merge_sql += StringUtil::Format("CAST(__o%llu AS %s) AS %s", static_cast<long long unsigned>(idx),
			                                output_types[idx].ToString(), SQLIdentifier::ToString(output_names[idx]));
		}
		merge_sql += " FROM __final";
		break;

	case QueryPlanAnalyzer::MergeStrategy::DISTINCT_MERGE:
		merge_sql = StringUtil::Format("SELECT DISTINCT * FROM %s", temp_table_name);
		break;

	default:
		// Shouldn't reach here
		merge_sql = StringUtil::Format("SELECT * FROM %s", temp_table_name);
		break;
	}

	// Execute the merge SQL and return the result
	auto final_result = conn.Query(merge_sql);

	// Clean up temp table
	conn.Query(StringUtil::Format("DROP TABLE IF EXISTS %s", temp_table_name));

	// Propagate final aggregation errors instead of falling back to partial results.
	return final_result;
}

arrow::Status ResultMerger::CollectResults(vector<std::unique_ptr<arrow::flight::FlightStreamReader>> &streams,
                                           const vector<string> &names, const vector<LogicalType> &types,
                                           std::shared_ptr<arrow::Schema> &schema,
                                           vector<std::shared_ptr<arrow::RecordBatch>> &batches) {
	// Workers and local execution encode results with the same converter options.
	auto client_properties = conn.context->GetClientProperties();
	client_properties.arrow_lossless_conversion = true;
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_properties);
	ARROW_ASSIGN_OR_RAISE(schema, arrow::ImportSchema(&arrow_schema));

	for (auto &stream : streams) {
		while (true) {
			ARROW_ASSIGN_OR_RAISE(auto next, stream->Next());
			if (!next.data) {
				break;
			}
			// Field metadata carries Arrow extension types, so it must match too.
			if (!next.data->schema()->Equals(*schema, /*check_metadata=*/true)) {
				return arrow::Status::Invalid("Worker result schema ", next.data->schema()->ToString(),
				                              " does not match expected schema ", schema->ToString());
			}
			batches.emplace_back(std::move(next.data));
		}
	}
	return arrow::Status::OK();
}

} // namespace duckdb
