#include "catch/catch.hpp"

#include "client/distributed_flight_client.hpp"
#include "distributed.pb.h"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckherder_extension.hpp"
#include "server/driver/distributed_flight_server.hpp"
#include "storage_config.hpp"
#include "utils/network_utils.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <system_error>
#include <thread>

using namespace duckdb; // NOLINT

namespace {

const std::string SERVER_HOST = "0.0.0.0";
const int SERVER_PORT = 18815;
const std::string SERVER_URL = "grpc://localhost:18815";

class FlightTestServer {
public:
	explicit FlightTestServer(int port = SERVER_PORT)
	    : server(std::make_unique<DistributedFlightServer>(SERVER_HOST, port)) {
		auto status = server->Start();
		if (!status.ok()) {
			throw std::runtime_error("Failed to start server: " + status.ToString());
		}
		server_thread = std::thread([this] {
			auto serve_status = server->Serve();
			if (!serve_status.ok()) {
				std::cerr << "Server error: " << serve_status.ToString() << std::endl;
			}
		});
		std::this_thread::sleep_for(std::chrono::seconds(2));
	}

	~FlightTestServer() {
		server->Shutdown();
		if (server_thread.joinable()) {
			server_thread.join();
		}
	}

	DistributedFlightServer &GetServer() {
		return *server;
	}

private:
	std::unique_ptr<DistributedFlightServer> server;
	std::thread server_thread;
};

FlightTestServer &GetTestServer() {
	static FlightTestServer test_server;
	return test_server;
}

// Scan all batches returned by the server and count their rows.
uint64_t CountRows(DistributedFlightClient &client, const string &table_name) {
	std::unique_ptr<arrow::flight::FlightStreamReader> stream;
	REQUIRE(client.ScanTable(table_name, 100, 0, stream).ok());

	uint64_t row_count = 0;
	while (true) {
		auto next = stream->Next();
		REQUIRE(next.ok());
		auto batch = next.ValueOrDie().data;
		if (!batch) {
			break;
		}
		row_count += batch->num_rows();
	}
	return row_count;
}

} // namespace

TEST_CASE("Test Flight server startup and connection", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	auto status = client.Connect();

	REQUIRE(status.ok());
}

TEST_CASE("Expired writer lease can be reclaimed", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	struct LeaseTimeoutReset {
		explicit LeaseTimeoutReset(DistributedFlightServer &server_p) : server(server_p) {
		}
		~LeaseTimeoutReset() {
			server.SetClientLeaseTimeoutForTesting(std::chrono::seconds(30));
		}
		DistributedFlightServer &server;
	} timeout_reset(server);

	server.SetClientLeaseTimeoutForTesting(std::chrono::milliseconds(1));
	DistributedFlightClient expired_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(expired_writer.Connect().ok());
	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	DistributedFlightClient replacement_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(replacement_writer.Connect().ok());
}

TEST_CASE("Server reset clears writer admission", "[distributed_flight]") {
	auto &server = GetTestServer().GetServer();
	DistributedFlightClient old_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(old_writer.Connect().ok());

	server.Reset();

	DistributedFlightClient replacement_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(replacement_writer.Connect().ok());

	bool exists = false;
	REQUIRE_FALSE(old_writer.TableExists("reset_invalidates_old_client", exists).ok());
}

TEST_CASE("Each client owns an isolated DuckDB connection", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	DistributedFlightClient reader(SERVER_URL, distributed::CLIENT_ROLE_READ_ONLY);
	REQUIRE(writer.Connect().ok());
	REQUIRE(reader.Connect().ok());

	distributed::DistributedResponse response;
	REQUIRE(writer.CreateTable("CREATE TABLE client_connection_isolation (id INTEGER)", response).ok());
	REQUIRE(response.success());
	REQUIRE(writer.ExecuteSQL("INSERT INTO client_connection_isolation VALUES (1)", response).ok());
	REQUIRE(response.success());

	REQUIRE(writer.ExecuteSQL("BEGIN TRANSACTION", response).ok());
	REQUIRE(response.success());
	REQUIRE(writer.ExecuteSQL("INSERT INTO client_connection_isolation VALUES (2)", response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(reader, "client_connection_isolation") == 1);

	// Closing the writer destroys its server-side connection and rolls back the open transaction.
	writer.Close();
	DistributedFlightClient replacement_writer(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(replacement_writer.Connect().ok());
	REQUIRE(CountRows(replacement_writer, "client_connection_isolation") == 1);

	REQUIRE(replacement_writer.ExecuteSQL("INSERT INTO client_connection_isolation VALUES (3)", response).ok());
	REQUIRE(response.success());
	REQUIRE(CountRows(reader, "client_connection_isolation") == 2);
}

TEST_CASE("Test TableExists via protobuf", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse create_resp;
	auto status = client.CreateTable("CREATE TABLE test_exists (id INTEGER)", create_resp);
	REQUIRE(status.ok());
	REQUIRE(create_resp.success());

	bool exists = false;
	status = client.TableExists("test_exists", exists);
	REQUIRE(status.ok());
	REQUIRE(exists);

	bool not_exists = false;
	status = client.TableExists("nonexistent_table", not_exists);
	REQUIRE(status.ok());
	REQUIRE_FALSE(not_exists);
}

TEST_CASE("Test error handling in protobuf responses", "[distributed_flight]") {
	GetTestServer();
	DistributedFlightClient client(SERVER_URL, distributed::CLIENT_ROLE_READ_WRITE);
	REQUIRE(client.Connect().ok());

	distributed::DistributedResponse response;
	auto status = client.ExecuteSQL("INVALID SQL SYNTAX", response);

	REQUIRE(status.ok());
	REQUIRE_FALSE(response.success());
	REQUIRE_FALSE(response.error_message().empty());
}

TEST_CASE("Client ATTACH initializes storage on a running driver", "[distributed_flight]") {
	struct TemporaryDatabase {
		std::filesystem::path path =
		    std::filesystem::temp_directory_path() /
		    StringUtil::Format("duckherder's-%s.db", UUID::ToString(UUID::GenerateRandomUUID()));
		~TemporaryDatabase() {
			std::error_code error;
			std::filesystem::remove(path, error);
		}
	} database;
	{
		DuckDB source(database.path.string());
		Connection source_conn(source);
		REQUIRE_FALSE(source_conn.Query("CREATE TABLE items AS SELECT i FROM range(3) t(i)")->HasError());
	}

	const auto port = GetAvailablePort(SERVER_PORT + 1);
	REQUIRE(port > 0);
	FlightTestServer server(port);
	DuckDB client_db(nullptr);
	ExtensionLoader loader(*client_db.instance, "duckherder");
	DuckherderExtension extension;
	extension.Load(loader);
	Connection connection(client_db);
	const auto attach_sql = StringUtil::Format(
	    "ATTACH ':memory:' AS dh (TYPE duckherder, server_host 'localhost', server_port %d, server_db_path %s)", port,
	    KeywordHelper::WriteQuoted(database.path.string()));
	// A failed attach must not occupy the single writable-client slot.
	REQUIRE(connection
	            .Query(StringUtil::Format(
	                "ATTACH ':memory:' AS failed (TYPE duckherder, server_host 'localhost', "
	                "server_port %d, server_db_path %s)",
	                port, KeywordHelper::WriteQuoted(StringUtil::Format("%s.missing", database.path.string()))))
	            ->HasError());
	REQUIRE_FALSE(connection.Query(attach_sql)->HasError());
	// Remote catalog discovery is not implemented; register the existing table's local metadata.
	REQUIRE_FALSE(connection.Query("CREATE TABLE dh.items (i BIGINT)")->HasError());
	REQUIRE_FALSE(connection.Query("PRAGMA duckherder_register_remote_table('items', 'items')")->HasError());
	auto rows = connection.Query("SELECT i FROM dh.items ORDER BY i");
	INFO((rows->HasError() ? rows->GetError() : ""));
	REQUIRE_FALSE(rows->HasError());
	REQUIRE(rows->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(rows->GetValue(0, 2).GetValue<int64_t>() == 2);

	StorageConfig config;
	config.database_uri = database.path.string();
	DistributedFlightClient reader(StringUtil::Format("grpc://localhost:%d", port), distributed::CLIENT_ROLE_READ_ONLY,
	                               config);
	REQUIRE(reader.Connect().ok());
	REQUIRE(CountRows(reader, "items") == 3);
	config.database_uri += ".different";
	DistributedFlightClient conflicting_reader(StringUtil::Format("grpc://localhost:%d", port),
	                                           distributed::CLIENT_ROLE_READ_ONLY, config);
	REQUIRE_FALSE(conflicting_reader.Connect().ok());
	REQUIRE(CountRows(reader, "items") == 3);
}
