#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct RemoteEndpoint {
	string host;
	int port;
	string database_name;
};

RemoteEndpoint ParseRemoteEndpoint(const string &path);

} // namespace duckdb
