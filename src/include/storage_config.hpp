#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// Storage location shared by client attachment requests and process startup hooks.
struct StorageConfig {
	string database_uri;
	string backend = "local";
	string root;
	string bucket;
};

} // namespace duckdb
