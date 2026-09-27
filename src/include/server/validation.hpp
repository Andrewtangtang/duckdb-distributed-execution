#pragma once

#include "client.pb.h"
#include "distributed.pb.h"
#include "transaction.pb.h"
#include "worker.pb.h"

#include <arrow/status.h>

namespace duckdb {

// Validate the shape and enum values of stateless protocol requests.
arrow::Status ValidateRequest(const distributed::DistributedRequest &request);
arrow::Status ValidateRequest(const distributed::RegisterClientRequest &request);
arrow::Status ValidateRequest(const distributed::TransactionRequest &request);
arrow::Status ValidateRequest(const distributed::WorkerRegisterRequest &request);

} // namespace duckdb
