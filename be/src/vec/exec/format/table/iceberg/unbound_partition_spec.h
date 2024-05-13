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

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "vec/exec/format/table/iceberg/partition_spec.h"
#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/exec/format/table/iceberg/transforms.h"
#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace iceberg {

struct UnboundPartitionField {
    std::unique_ptr<Transform> transform;
    int source_id;
    const int partition_id;
    std::string name;

    UnboundPartitionField(const std::string& transform_as_string, int source_id, int partition_id,
                          const std::string& name)
            : transform(Transforms::from_string(transform_as_string)),
              source_id(source_id),
              partition_id(partition_id),
              name(name) {}
};

class UnboundPartitionSpec {
public:
    class Builder {
    public:
        Builder& with_spec_id(int new_spec_id) {
            _spec_id = new_spec_id;
            return *this;
        }

        Builder& add_field(const std::string& transform_as_string, int source_id,
                           const std::string& name) {
            _fields.emplace_back(transform_as_string, source_id, -1, name);
            return *this;
        }

        Builder& add_field(const std::string& transform_as_string, int source_id, int partition_id,
                           const std::string& name) {
            _fields.emplace_back(transform_as_string, source_id, partition_id, name);
            return *this;
        }

        std::unique_ptr<UnboundPartitionSpec> build() {
            return std::make_unique<UnboundPartitionSpec>(_spec_id, std::move(_fields));
        }

    private:
        int _spec_id = 0;
        std::vector<UnboundPartitionField> _fields;
    };

    UnboundPartitionSpec(int specId, std::vector<UnboundPartitionField> fields)
            : _spec_id(specId), _fields(std::move(fields)) {}

    std::unique_ptr<PartitionSpec> bind(std::shared_ptr<Schema> schema) const {
        std::unique_ptr<PartitionSpec::Builder> builder = copy_to_builder(schema);
        return builder->build();
    }

    int spec_id() const { return _spec_id; }

private:
    std::unique_ptr<PartitionSpec::Builder> copy_to_builder(std::shared_ptr<Schema> schema) const {
        std::unique_ptr<PartitionSpec::Builder> builder =
                std::make_unique<PartitionSpec::Builder>(schema);
        for (const auto& field : _fields) {
            Type* fieldType = schema->find_type(field.source_id);
            std::unique_ptr<Transform> transform;
            if (fieldType) {
                transform = Transforms::from_string(fieldType, field.transform->to_string());
            } else {
                transform = Transforms::from_string(field.transform->to_string());
            }
            if (field.partition_id != -1) {
                builder->add(field.source_id, field.partition_id, field.name, std::move(transform));
            } else {
                builder->add(field.source_id, field.name, std::move(transform));
            }
        }
        return builder;
    }

    int _spec_id;
    std::vector<UnboundPartitionField> _fields;
};

} // namespace iceberg
} // namespace doris