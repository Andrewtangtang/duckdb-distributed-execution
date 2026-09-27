#pragma once

#include "duckdb/common/string.hpp"
#include "storage_config.hpp"

namespace duckdb {

// Forward declarations.
class PhysicalOperator;
class LogicalOperator;
class Connection;

// Read optional storage arguments after the executable's host, port, and worker arguments.
StorageConfig GetStorageConfig(int argc, char *argv[]);

// Attach a native file or configure ObjFS as object_db read-only before accepting queries.
void InitializeStorage(Connection &conn, const StorageConfig &config);

// Return true if there's any TABLE_SCAN operator in the physical plan tree.
bool ContainsTableScan(const PhysicalOperator &op);

// Return true if the logical plan contains only supported operators.
bool IsSupportedPlan(LogicalOperator &op);

} // namespace duckdb
