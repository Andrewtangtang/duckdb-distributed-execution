#pragma once

#include "client.pb.h"
#include "duckdb/main/connection.hpp"

namespace duckdb {

// Forward declarations.
class PhysicalOperator;
class LogicalOperator;

// Attach a native file or configure ObjFS as object_db read-only before accepting queries.
void InitializeStorage(Connection &conn, const distributed::StorageConfig &config);

// Return true if there's any TABLE_SCAN operator in the physical plan tree.
bool ContainsTableScan(const PhysicalOperator &op);

// Return true if the logical plan contains only supported operators.
bool IsSupportedPlan(LogicalOperator &op);

} // namespace duckdb
