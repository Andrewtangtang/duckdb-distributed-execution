#include "server/driver/query_utils.hpp"

#include "client.pb.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

void InitializeStorage(Connection &conn, const distributed::StorageConfig &config, bool read_only) {
	const auto backend = config.backend().empty() ? "local" : config.backend();
	if (config.database_uri().empty()) {
		if (!config.root().empty() || backend != "local") {
			throw InvalidInputException("Storage settings require a database path or URI");
		}
		return;
	}
	auto &fs = FileSystem::GetFileSystem(*conn.context);
	// IF NOT EXISTS only checks the alias; verify that it still identifies the requested database.
	auto existing = DatabaseManager::Get(*conn.context).GetDatabase("object_db");
	if (existing) {
		if (existing->GetCatalog().GetDBPath() != fs.CanonicalizePath(config.database_uri()) ||
		    existing->IsReadOnly() != read_only) {
			throw InvalidInputException("object_db is already attached to a different database or access mode");
		}
	}

	auto execute = [&](const string &sql) {
		auto result = conn.Query(sql);
		if (result->HasError()) {
			throw IOException("Storage initialization failed: %s", result->GetError());
		}
	};
	if (!StringUtil::StartsWith(config.database_uri(), "duckdb_objfs://")) {
		throw InvalidInputException("Storage database URI must use duckdb_objfs://");
	}
	if (config.database_uri() == "duckdb_objfs://") {
		throw InvalidInputException("Object storage database URI must name a database");
	}
	if (backend != "local" || config.root().empty()) {
		throw InvalidInputException("Local object storage requires the local backend and a root");
	}
	if (!fs.IsPathAbsolute(config.root())) {
		throw InvalidInputException("Local object storage root must be an absolute path");
	}
	if (existing) {
		return;
	}
	execute(StringUtil::Format("SET duckdb_objfs_backend = %s", KeywordHelper::WriteQuoted(backend)));
	execute(StringUtil::Format("SET duckdb_objfs_root = %s", KeywordHelper::WriteQuoted(config.root())));
	execute(StringUtil::Format("ATTACH IF NOT EXISTS %s AS object_db%s",
	                           KeywordHelper::WriteQuoted(config.database_uri()), read_only ? " (READ_ONLY)" : ""));
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
