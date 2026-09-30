#include "server/driver/client_registration.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/query_utils.hpp"
#include "server/driver/worker_manager.hpp"
#include "utils/time_utils.hpp"

namespace duckdb {

ClientRegistration::ClientRegistration(DuckDB &db, WorkerManager &worker_manager, distributed::ClientRole role_p,
                                       const distributed::StorageConfig &storage_config)
    : role(role_p), last_seen(GetSteadyNowMilliSecSinceEpoch()), connection(make_uniq<Connection>(db)) {
	InitializeStorage(*connection, storage_config, /*read_only=*/role == distributed::CLIENT_ROLE_READ_ONLY);
	auto use_result = connection->Query(storage_config.database_uri().empty() ? "USE duckling" : "USE object_db");
	if (use_result->HasError()) {
		throw InternalException(
		    StringUtil::Format("Failed to select database for client connection: %s", use_result->GetError()));
	}
	distributed_executor = make_uniq<DistributedExecutor>(worker_manager, *connection);
}

ClientRegistration::~ClientRegistration() = default;

} // namespace duckdb
