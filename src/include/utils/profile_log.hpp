#pragma once

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/connection.hpp"

#include <cstdlib>
#include <fstream>

namespace duckdb {

// Timing output for benchmark profiling, written only when DUCKHERDER_PROFILE_DIR is set. The driver and the workers
// share the directory, so they must run on one machine or a shared filesystem.
inline string ProfileDir() {
	const char *dir = std::getenv("DUCKHERDER_PROFILE_DIR");
	return dir ? string(dir) : string();
}

inline int64_t EpochMillis() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

// Appends one tab-separated line to <dir>/<file>. Lines are short, so concurrent appends from processes stay whole.
inline void AppendProfileLine(const string &dir, const string &file, const string &line) {
	std::ofstream out(dir + "/" + file, std::ios::app);
	out << line << '\n';
}

// Makes the next queries on conn write their JSON profile, with per-operator timings, to <dir>/<name>.json. Sets the
// client config directly, so no profiled SET statement overwrites an earlier profile.
inline void EnableQueryProfile(Connection &conn, const string &dir, const string &name) {
	auto &config = ClientConfig::GetConfig(*conn.context);
	config.enable_profiler = true;
	config.emit_profiler_output = true;
	config.profiler_print_format = ProfilerPrintFormat::JSON;
	config.profiler_save_location = dir + "/" + name + ".json";
}

} // namespace duckdb
