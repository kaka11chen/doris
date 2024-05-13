// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <optional>
#include <regex>

#include "common/exception.h"
#include "vec/columns/column.h"
#include "vec/exec/format/table/iceberg/partition_spec.h"
#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace vectorized {

class PartitionColumnTransform {
    using BlockTransformFunctionType = std::function<IColumn&(IColumn&)>;
    using ValueTransformFunctionType = std::function<std::optional<int64_t>(IColumn&, int)>;

public:
    PartitionColumnTransform(doris::iceberg::Type& type, bool preserves_non_null, bool monotonic,
                             bool temporal, const BlockTransformFunctionType& block_transform,
                             const ValueTransformFunctionType& value_transform)
            : _type(type),
              _preserves_non_null(preserves_non_null),
              _monotonic(monotonic),
              _temporal(temporal),
              _block_transform(block_transform),
              _value_transform(value_transform) {}

    static PartitionColumnTransform create(const iceberg::PartitionField& field,
                                           doris::iceberg::Type& source_type);

    doris::iceberg::Type& type() const { return _type; }

    bool preservesNonNull() const { return _preserves_non_null; }

    bool monotonic() const { return _monotonic; }

    bool temporal() const { return _temporal; }

    const BlockTransformFunctionType& block_transform() const { return _block_transform; }

    const ValueTransformFunctionType& value_transform() const { return _value_transform; }

private:
    doris::iceberg::Type& _type;
    bool _preserves_non_null;
    bool _monotonic;
    bool _temporal;
    const std::function<IColumn&(IColumn&)> _block_transform;
    ValueTransformFunctionType _value_transform;
};

} // namespace vectorized
} // namespace doris