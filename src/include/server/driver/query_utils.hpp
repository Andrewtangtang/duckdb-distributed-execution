#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Forward declarations.
class PhysicalOperator;
class LogicalOperator;
class Connection;

// Explicit storage settings shared by driver and worker startup hooks.
struct ObjectStorageConfig {
	string database_uri;
	string backend = "local";
	string root;
	string bucket;
};

// Read optional storage arguments after the executable's host, port, and worker arguments.
ObjectStorageConfig GetObjectStorageConfig(int argc, char *argv[]);

// Configure ObjFS and attach object_db read-only before accepting queries.
void InitializeObjectStorage(Connection &conn, const ObjectStorageConfig &config);

// Return true if there's any TABLE_SCAN operator in the physical plan tree.
bool ContainsTableScan(const PhysicalOperator &op);

// Return true if the logical plan contains only supported operators.
bool IsSupportedPlan(LogicalOperator &op);

} // namespace duckdb
