#pragma once

#include <arrow/status.h>

namespace duckdb {
namespace distributed {
class DistributedRequest;
class RegisterClientRequest;
class TransactionRequest;
class WorkerRegisterRequest;
} // namespace distributed

// Validate the shape and enum values of stateless protocol requests.
arrow::Status ValidateRequest(const distributed::DistributedRequest &request);
arrow::Status ValidateRequest(const distributed::RegisterClientRequest &request);
arrow::Status ValidateRequest(const distributed::TransactionRequest &request);
arrow::Status ValidateRequest(const distributed::WorkerRegisterRequest &request);

} // namespace duckdb
