#include <algorithm>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace doris {
namespace vectorized {

class VHiveUtils {
public:
    static const std::string DEFAULT_DYNAMIC_PARTITION;

    static const std::regex PATH_CHAR_TO_ESCAPE;

    static std::string make_partition_name(const std::vector<std::string>& columns,
                                           const std::vector<std::string>& values);

    static std::string escape_path_name(const std::string& path);
};
} // namespace vectorized
} // namespace doris
