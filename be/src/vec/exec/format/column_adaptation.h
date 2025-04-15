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

#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/exec/format/format_common.h"

namespace doris::vectorized {

class ColumnAdaptation {
public:
    virtual ColumnWithTypeAndName column(const Block& block) = 0;

    virtual ~ColumnAdaptation() = default;
};

class NullColumn : public ColumnAdaptation {
public:
    explicit NullColumn(std::string name, const DataTypePtr& type) : _name(std::move(name)), _type(type) {
        auto column = type->create_column();
        column->resize(1);
        _null_column = ColumnNullable::create(std::move(column), ColumnUInt8::create(1, 1));
    }

    ColumnWithTypeAndName column(const Block& block) override {
        if (!_null_column) {
            throw std::runtime_error("Null column is not initialized");
        }
        ColumnWithTypeAndName column_with_type_and_name;
        column_with_type_and_name.column = ColumnConst::create(_null_column, block.rows());
        column_with_type_and_name.name = _name;
        column_with_type_and_name.type = _type;
        return column_with_type_and_name;
    }

private:
    std::string _name;
    DataTypePtr _type;
    ColumnPtr _null_column;
};

class SourceColumn : public ColumnAdaptation {
public:
    explicit SourceColumn(int index) : _index(index) {
        DCHECK(index >= 0);
    }

    ColumnWithTypeAndName column(const Block& block) override {
        return block.get_by_position(_index);
    }
private:
    int _index;
};

class ConstantAdaptation : public ColumnAdaptation {
public:
    explicit ConstantAdaptation(const ColumnWithTypeAndName& single_value_block) : _single_value_block(single_value_block) {
    }

    ColumnWithTypeAndName column(const Block& block) override {
        ColumnWithTypeAndName column_with_type_and_name;
        column_with_type_and_name.column = ColumnConst::create(_single_value_block.column, block.rows());
        column_with_type_and_name.name = _single_value_block.name;
        column_with_type_and_name.type = _single_value_block.type;
        return column_with_type_and_name;
    }

private:
    ColumnWithTypeAndName _single_value_block;
};


} // namespace doris::vectorized