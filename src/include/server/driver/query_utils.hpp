#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Forward declarations.
class PhysicalOperator;
class LogicalOperator;
class Connection;

// Execute a local SQL file before accepting queries.
void InitializeConnection(Connection &conn, const string &sql_file);

// Return true if there's any TABLE_SCAN operator in the physical plan tree.
bool ContainsTableScan(const PhysicalOperator &op);

// Return true if the logical plan contains only supported operators.
bool IsSupportedPlan(LogicalOperator &op);

} // namespace duckdb
