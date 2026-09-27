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

void InitializeStorage(Connection &conn, const distributed::StorageConfig &config) {
	const auto backend = config.backend().empty() ? "local" : config.backend();
	if (config.database_uri().empty()) {
		if (!config.root().empty() || !config.bucket().empty() || backend != "local") {
			throw InvalidInputException("Storage settings require a database path or URI");
		}
		return;
	}
	// IF NOT EXISTS only checks the alias; verify that it still identifies the requested read-only database.
	auto existing = DatabaseManager::Get(*conn.context).GetDatabase("object_db");
	if (existing) {
		auto &fs = FileSystem::GetFileSystem(*conn.context);
		if (existing->GetCatalog().GetDBPath() != fs.CanonicalizePath(config.database_uri()) ||
		    !existing->IsReadOnly()) {
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
		if (config.database_uri().find("://") != string::npos || backend != "local" || !config.root().empty() ||
		    !config.bucket().empty()) {
			throw InvalidInputException("Native database files do not accept object storage settings or URI schemes");
		}
		execute(StringUtil::Format("ATTACH IF NOT EXISTS %s AS object_db (READ_ONLY)",
		                           KeywordHelper::WriteQuoted(config.database_uri())));
		return;
	}
	if (config.database_uri() == "duckdb_objfs://") {
		throw InvalidInputException("Object storage database URI must name a database");
	}
	if (backend != "local" && backend != "s3") {
		throw InvalidInputException("Object storage backend must be local or s3");
	}
	if (backend == "local" && (config.root().empty() || !config.bucket().empty())) {
		throw InvalidInputException("Local object storage requires a root and does not accept a bucket");
	}
	if (backend == "s3" && config.bucket().empty()) {
		throw InvalidInputException("S3 object storage requires a bucket");
	}
	execute("LOAD duckdb_object_storage");
	execute(StringUtil::Format("SET duckdb_objfs_backend = %s", KeywordHelper::WriteQuoted(backend)));
	if (!config.root().empty()) {
		execute(StringUtil::Format("SET duckdb_objfs_root = %s", KeywordHelper::WriteQuoted(config.root())));
	}
	if (backend == "s3") {
		execute("LOAD cache_httpfs");
		// Credentials are resolved locally, never sent in worker registration requests.
		execute(StringUtil::Format(
		    "CREATE SECRET IF NOT EXISTS duckherder_objfs (TYPE S3, PROVIDER credential_chain, SCOPE %s)",
		    KeywordHelper::WriteQuoted(StringUtil::Format("s3://%s/", config.bucket()))));
		execute(StringUtil::Format("SET duckdb_objfs_bucket = %s", KeywordHelper::WriteQuoted(config.bucket())));
	}
	execute(StringUtil::Format("ATTACH IF NOT EXISTS %s AS object_db (READ_ONLY)",
	                           KeywordHelper::WriteQuoted(config.database_uri())));
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
