#include "server/driver/query_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <fstream>
#include <iterator>
#include <utility>

namespace duckdb {

void InitializeConnection(Connection &conn, const string &sql_file) {
	if (sql_file.empty()) {
		return;
	}
	std::ifstream input(sql_file);
	if (!input) {
		throw IOException("Cannot open initialization SQL file: %s", sql_file);
	}
	string sql((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
	if (input.bad()) {
		throw IOException("Cannot read initialization SQL file: %s", sql_file);
	}
	auto statements = conn.ExtractStatements(sql);
	for (auto &statement : statements) {
		auto result = conn.Query(std::move(statement));
		if (result->HasError()) {
			throw IOException("Initialization SQL failed: %s", result->GetError());
		}
	}
}

bool ContainsTableScan(const PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsTableScan(child.get())) {
			return true;
		}
	}
	return false;
}

bool IsSupportedPlan(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		if (op.children.size() != 1) {
			return false;
		}
		return IsSupportedPlan(*op.children[0]);
	}
	case LogicalOperatorType::LOGICAL_GET:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
