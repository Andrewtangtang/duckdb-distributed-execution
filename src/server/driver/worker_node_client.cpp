#include "server/driver/worker_node_client.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>

namespace duckdb {

WorkerNodeClient::WorkerNodeClient(const string &location_p) : location(std::move(location_p)) {
}

arrow::Status WorkerNodeClient::Connect() {
	arrow::flight::Location flight_location;
	ARROW_ASSIGN_OR_RAISE(flight_location, arrow::flight::Location::Parse(location));
	ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(flight_location));
	return arrow::Status::OK();
}

arrow::Status WorkerNodeClient::ExecutePartition(const distributed::ExecutePartitionRequest &request,
                                                 arrow::RecordBatchVector &batches) {
	const auto start = std::chrono::steady_clock::now();
	distributed::DistributedRequest req;
	*req.mutable_execute_partition() = request;

	const auto req_data = req.SerializeAsString();
	// DoGet executes the partition and propagates worker errors. The previous DoAction only acknowledged the request.
	arrow::flight::Ticket ticket;
	ticket.ticket = req_data;
	ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(ticket));
	const auto get_end = std::chrono::steady_clock::now();
	ARROW_ASSIGN_OR_RAISE(batches, stream->ToRecordBatches());
	if (std::getenv("DUCKHERDER_PROFILE_DISTRIBUTED")) {
		const auto end = std::chrono::steady_clock::now();
		auto ms = [](auto a, auto b) {
			return std::chrono::duration<double, std::milli>(b - a).count();
		};
		std::fprintf(stderr, "PROFILE rpc task=%llu get=%.3f fetch=%.3f total=%.3f ms\n",
		             static_cast<unsigned long long>(request.partition_id()), ms(start, get_end), ms(get_end, end),
		             ms(start, end));
	}
	return arrow::Status::OK();
}

} // namespace duckdb
