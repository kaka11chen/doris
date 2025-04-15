#include "vec/columns/dictionary_column.h"

namespace doris::vectorized {

// 静态成员初始化
std::string DictionaryId::nodeId;
std::atomic<long> DictionaryId::sequenceGenerator{0};

} // namespace doris::vectorized