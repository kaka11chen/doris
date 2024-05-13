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

#include <atomic>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/exec/format/table/iceberg/struct_like.h"
#include "vec/exec/format/table/iceberg/transforms.h"

namespace doris {
namespace iceberg {

class PartitionField {
public:
    PartitionField(int sourceId, int fieldId, std::string name,
                   std::unique_ptr<Transform> transform)
            : _source_id(sourceId),
              _field_id(fieldId),
              _name(std::move(name)),
              _transform(std::move(transform)) {}

    int source_id() const { return _source_id; }

    int field_id() const { return _field_id; }

    const std::string& name() const { return _name; }

    const Transform& transform() const { return *_transform; }

private:
    int _source_id;
    int _field_id;
    std::string _name;
    std::unique_ptr<Transform> _transform;
};

class PartitionSpec {
public:
    class Builder {
    public:
        Builder(std::shared_ptr<Schema> schema)
                : _schema(std::move(schema)),
                  _spec_id(0),
                  _last_assigned_field_id(PARTITION_DATA_ID_START - 1) {}

        ~Builder() = default;

        Builder& with_spec_id(int new_spec_id) {
            _spec_id = new_spec_id;
            return *this;
        }

        Builder& add(int sourceId, int fieldId, const std::string& name,
                     std::unique_ptr<Transform> transform) {
            _fields.emplace_back(sourceId, fieldId, name, std::move(transform));
            _last_assigned_field_id.store(std::max(_last_assigned_field_id.load(), fieldId));
            return *this;
        }

        Builder& add(int sourceId, const std::string& name, std::unique_ptr<Transform> transform) {
            return add(sourceId, nextFieldId(), name, std::move(transform));
        }

        std::unique_ptr<PartitionSpec> build() {
            return std::make_unique<PartitionSpec>(_schema, _spec_id, std::move(_fields),
                                                   _last_assigned_field_id.load());
        }

    private:
        int nextFieldId() { return ++_last_assigned_field_id; }

    private:
        std::shared_ptr<Schema> _schema;
        std::vector<PartitionField> _fields;
        int _spec_id;
        std::atomic<int> _last_assigned_field_id;
    };

    PartitionSpec(std::shared_ptr<Schema> schema, int spec_id, std::vector<PartitionField> fields,
                  int last_assigned_field_id)
            : _schema(std::move(schema)),
              _spec_id(spec_id),
              _fields(std::move(fields)),
              _last_assigned_field_id(last_assigned_field_id) {}

    const Schema& schema() const { return *_schema; }

    int spec_id() { return _spec_id; }

    const std::vector<PartitionField>& fields() const { return _fields; }

    bool is_partitioned() const {
        return !_fields.empty() &&
               std::any_of(_fields.begin(), _fields.end(),
                           [](const PartitionField& f) { return !f.transform().is_void(); });
    }

    bool is_unpartitioned() const { return !is_partitioned(); }

    int last_assigned_field_id() const { return _last_assigned_field_id; }

    std::string escape(const std::string& str) { return str; }

    std::string partition_to_path(const StructLike& data) {
        std::stringstream ss;

        for (size_t i = 0; i < _fields.size(); i++) {
            Type* source_type = _schema->find_type(_fields[i].source_id());
            Type& result_type = _fields[i].transform().get_result_type(*source_type);
            std::string value_string = _fields[i].transform().to_human_string(
                    result_type, data.get(i, result_type.type_id()));
            if (i > 0) {
                ss << "/";
            }
            ss << escape(_fields[i].name()) << '=' << escape(value_string);
        }

        return ss.str();
    }

    std::vector<std::string> partition_values(const StructLike& data) {
        std::vector<std::string> partition_values;
        partition_values.reserve(_fields.size());
        for (size_t i = 0; i < _fields.size(); i++) {
            Type* source_type = _schema->find_type(_fields[i].source_id());
            Type& result_type = _fields[i].transform().get_result_type(*source_type);
            partition_values.emplace_back(_fields[i].transform().to_human_string(
                    result_type, data.get(i, result_type.type_id())));
        }
        return partition_values;
    }

private:
    // IDs for partition fields start at 1000
    static constexpr int PARTITION_DATA_ID_START = 1000;
    std::shared_ptr<Schema> _schema;
    int _spec_id;
    std::vector<PartitionField> _fields;
    int _last_assigned_field_id;
};

} // namespace iceberg
} // namespace doris
