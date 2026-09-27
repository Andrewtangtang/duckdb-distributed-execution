#include "server/driver/query_utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

ObjectStorageConfig GetObjectStorageConfig(int argc, char *argv[]) {
	if (argc > 8) {
		throw InvalidInputException(
		    "Expected [database_uri] [backend] [root] [bucket] after host, port, and worker arguments");
	}
	ObjectStorageConfig config;
	if (argc > 4) {
		config.database_uri = argv[4];
	}
	if (argc > 5) {
		config.backend = argv[5];
	}
	if (argc > 6) {
		config.root = argv[6];
	}
	if (argc > 7) {
		config.bucket = argv[7];
	}
	return config;
}

void InitializeObjectStorage(Connection &conn, const ObjectStorageConfig &config) {
	if (config.database_uri.empty()) {
		if (!config.root.empty() || !config.bucket.empty() || config.backend != "local") {
			throw InvalidInputException("Object storage settings require a database URI");
		}
		return;
	}
	if (!StringUtil::StartsWith(config.database_uri, "duckdb_objfs://") || config.database_uri == "duckdb_objfs://") {
		throw InvalidInputException("Object storage database URI must start with duckdb_objfs:// and name a database");
	}
	if (config.backend != "local" && config.backend != "s3") {
		throw InvalidInputException("Object storage backend must be local or s3");
	}
	if (config.backend == "local" && (config.root.empty() || !config.bucket.empty())) {
		throw InvalidInputException("Local object storage requires a root and does not accept a bucket");
	}
	if (config.backend == "s3" && config.bucket.empty()) {
		throw InvalidInputException("S3 object storage requires a bucket");
	}
	auto execute = [&](const string &sql) {
		auto result = conn.Query(sql);
		if (result->HasError()) {
			throw IOException("Object storage initialization failed: %s", result->GetError());
		}
	};
	execute("LOAD duckdb_object_storage");
	execute(StringUtil::Format("SET duckdb_objfs_backend = %s", KeywordHelper::WriteQuoted(config.backend)));
	if (!config.root.empty()) {
		execute(StringUtil::Format("SET duckdb_objfs_root = %s", KeywordHelper::WriteQuoted(config.root)));
	}
	if (config.backend == "s3") {
		execute("LOAD cache_httpfs");
		// Credentials are resolved locally, never sent in worker registration requests.
		execute(StringUtil::Format("CREATE SECRET duckherder_objfs (TYPE S3, PROVIDER credential_chain, SCOPE %s)",
		                           KeywordHelper::WriteQuoted(StringUtil::Format("s3://%s/", config.bucket))));
		execute(StringUtil::Format("SET duckdb_objfs_bucket = %s", KeywordHelper::WriteQuoted(config.bucket)));
	}
	execute(StringUtil::Format("ATTACH %s AS object_db (READ_ONLY)", KeywordHelper::WriteQuoted(config.database_uri)));
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
