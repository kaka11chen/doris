#include "vhive_utils.h"

#include <algorithm>
#include <regex>
#include <sstream>

namespace doris {
namespace vectorized {

const std::string VHiveUtils::DEFAULT_DYNAMIC_PARTITION = "__HIVE_DEFAULT_PARTITION__";
const std::regex VHiveUtils::PATH_CHAR_TO_ESCAPE("[\\x00-\\x1F\"#%'*/:=?\\\\\\x7F\\{\\[\\]\\^]");

std::string VHiveUtils::make_partition_name(const std::vector<THiveColumn>& columns,
                                            const std::vector<int>& partition_columns_input_index,
                                            const std::vector<std::string>& values) {
    std::stringstream partitionNameStream;

    for (size_t i = 0; i < partition_columns_input_index.size(); i++) {
        if (i > 0) {
            partitionNameStream << '/';
        }
        std::string column = columns[partition_columns_input_index[i]].name;
        std::string value = values[i];
        // Convert column to lowercase using English locale
        std::transform(column.begin(), column.end(), column.begin(),
                       [&](char c) { return std::tolower(c); });
        partitionNameStream << escape_path_name(column) << '=' << escape_path_name(value);
    }

    return partitionNameStream.str();
}

std::string VHiveUtils::escape_path_name(const std::string& path) {
    if (path.empty()) {
        return DEFAULT_DYNAMIC_PARTITION;
    }

    // Fast-path detection, no escaping and therefore no copying necessary
    std::smatch match;
    if (!std::regex_search(path, match, PATH_CHAR_TO_ESCAPE)) {
        return path;
    }

    // Slow path, escape beyond the first required escape character into a new string
    std::stringstream sb;
    size_t fromIndex = 0;
    auto begin = path.begin(); // Iterator for the beginning of the string
    auto end = path.end();     // Iterator for the end of the string
    while (std::regex_search(begin + fromIndex, end, match, PATH_CHAR_TO_ESCAPE)) {
        size_t escapeAtIndex = match.position() + fromIndex;
        // preceding characters without escaping needed
        if (escapeAtIndex > fromIndex) {
            sb << path.substr(fromIndex, escapeAtIndex - fromIndex);
        }
        // escape single character
        char c = path[escapeAtIndex];
        sb << '%' << std::hex << std::uppercase << static_cast<int>(c >> 4)
           << static_cast<int>(c & 0xF);
        // find next character to escape
        fromIndex = escapeAtIndex + 1;
    }
    // trailing characters without escaping needed
    if (fromIndex < path.length()) {
        sb << path.substr(fromIndex);
    }
    return sb.str();
}
} // namespace vectorized
} // namespace doris