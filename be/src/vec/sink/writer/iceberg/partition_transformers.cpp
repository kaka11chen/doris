#include "vec/sink/writer/iceberg/partition_transformers.h"

namespace doris {
namespace vectorized {

PartitionColumnTransform PartitionColumnTransform::create(const iceberg::PartitionField& field,
                                                          doris::iceberg::Type& source_type) {
    std::string transform = field.transform().to_string();

    if (transform == "identity") {
        return PartitionColumnTransform(
                source_type, false, true, false, [](IColumn& column) -> IColumn& { return column; },
                [](IColumn& column, int position) -> std::optional<int64_t> {
                    return std::nullopt;
                });
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Unsupported partition transform: {}.", transform);
    }
}

} // namespace vectorized
} // namespace doris