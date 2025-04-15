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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeLazy.cpp
// and modified by Doris

#include "vec/data_types/data_type_lazy.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <streamvbyte.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "agent/be_exec_version_manager.h"
#include "common/cast_set.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/lazy_column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
DataTypeLazy::DataTypeLazy(const DataTypePtr& nested_data_type_)
        : nested_data_type {nested_data_type_} {
    if (!nested_data_type) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "DataTypeLazy input nested type is nullptr");
    }
}

std::string DataTypeLazy::to_string(const IColumn& column, size_t row_num) const {
//    auto result = check_column_const_set_readability(column, row_num);
//    ColumnPtr ptr = result.first;
//    row_num = result.second;
//
//    const auto& col_lazy = assert_cast<const LazyColumn&>(*ptr);
//    if (col_lazy.is_null_at(row_num)) {
//        return "NULL";
//    } else {
//        return get_nested_type()->to_string(col_lazy.get_column(), row_num);
//    }
    return "";
}

void DataTypeLazy::to_string(const IColumn& column, size_t row_num,
                                 BufferWritable& ostr) const {
//    auto result = check_column_const_set_readability(column, row_num);
//    ColumnPtr ptr = result.first;
//    row_num = result.second;
//
//    const auto& col_null = assert_cast<const LazyColumn&>(*ptr);
//    if (col_null.is_null_at(row_num)) {
//        ostr.write("NULL", 4);
//    } else {
//        get_nested_type()->to_string(col_null.get_nested_column(), row_num, ostr);
//    }
}

Status DataTypeLazy::from_string(ReadBuffer& rb, IColumn* column) const {
//    auto* null_column = assert_cast<LazyColumn*>(column);
//    if (rb.count() == 4 && *(rb.position()) == 'N' && *(rb.position() + 1) == 'U' &&
//        *(rb.position() + 2) == 'L' && *(rb.position() + 3) == 'L') {
//        null_column->insert_data(nullptr, 0);
//        return Status::OK();
//    }
//    auto st = nested_data_type->from_string(rb, &(null_column->get_nested_column()));
//    if (!st.ok()) {
//        // fill null if fail
//        null_column->insert_data(nullptr, 0); // 0 is meaningless here
//        return Status::OK();
//    }
//    // fill not null if succ
//    null_column->get_null_map_data().push_back(0);
//    return Status::OK();

        return Status::OK();
}

// binary: const flag | row num | read saved num| <null array> | <values array>
//  <null array>: is_null1 | is_null2 | ...
//  <values array>: value1 | value2 | ...>
int64_t DataTypeLazy::get_uncompressed_serialized_bytes(const IColumn& column,
                                                            int be_exec_version) const {
//    if (be_exec_version >= USE_CONST_SERDE) {
//        auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
//        bool is_const_column = is_column_const(column);
//        auto real_need_copy_num = is_const_column ? 1 : column.size();
//        const IColumn* data_column = &column;
//        if (is_const_column) {
//            const auto& const_column = assert_cast<const ColumnConst&>(column);
//            data_column = &(const_column.get_data_column());
//        }
//
//        const auto mem_size = real_need_copy_num * sizeof(bool);
//        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
//            size += mem_size;
//        } else {
//            // Throw exception if mem_size is large than UINT32_MAX
//            size = size + sizeof(size_t) +
//                   std::max(mem_size, streamvbyte_max_compressedbytes(
//                                              cast_set<UInt32>(upper_int32(mem_size))));
//        }
//        const auto& col = assert_cast<const ColumnLazy&>(*data_column);
//        size = size + nested_data_type->get_uncompressed_serialized_bytes(col.get_nested_column(),
//                                                                          be_exec_version);
//        return size;
//    } else {
//        size_t ret = 0;
//        if (size_t size = sizeof(bool) * column.size(); size <= SERIALIZED_MEM_SIZE_LIMIT) {
//            ret += size + sizeof(uint32_t);
//        } else {
//            // Throw exception if mem_size is large than UINT32_MAX
//            ret += (sizeof(uint32_t) + sizeof(size_t) +
//                    std::max(size,
//                             streamvbyte_max_compressedbytes(cast_set<UInt32>(upper_int32(size)))));
//        }
//        ret += nested_data_type->get_uncompressed_serialized_bytes(
//                assert_cast<const LazyColumn&>(*column.convert_to_full_column_if_const())
//                        .get_nested_column(),
//                be_exec_version);
//        return ret;
//    }

        return 0;
}

char* DataTypeLazy::serialize(const IColumn& column, char* buf, int be_exec_version) const {
//    if (be_exec_version >= USE_CONST_SERDE) {
//        const auto* data_column = &column;
//        size_t real_need_copy_num = 0;
//        buf = serialize_const_flag_and_row_num(&data_column, buf, &real_need_copy_num);
//
//        // mem_size = real_row_num * sizeof(T)
//        const auto mem_size = real_need_copy_num * sizeof(bool);
//        const auto& col = assert_cast<const LazyColumn&>(*data_column);
//        // null flags
//        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
//            memcpy(buf, col.get_null_map_data().data(), mem_size);
//            buf += mem_size;
//        } else {
//            // Throw exception if mem_size is large than UINT32_MAX
//            auto encode_size = streamvbyte_encode(
//                    reinterpret_cast<const uint32_t*>(col.get_null_map_data().data()),
//                    cast_set<UInt32>(upper_int32(mem_size)), (uint8_t*)(buf + sizeof(size_t)));
//            *reinterpret_cast<size_t*>(buf) = encode_size;
//            buf += (sizeof(size_t) + encode_size);
//        }
//        // data values
//        return nested_data_type->serialize(col.get_nested_column(), buf, be_exec_version);
//    } else {
//        auto ptr = column.convert_to_full_column_if_const();
//        const auto& col = assert_cast<const LazyColumn&>(*ptr.get());
//
//        // row num
//        auto mem_size = col.size() * sizeof(bool);
//        *reinterpret_cast<uint32_t*>(buf) = static_cast<UInt32>(mem_size);
//        buf += sizeof(uint32_t);
//        // null flags
//        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
//            memcpy(buf, col.get_null_map_data().data(), mem_size);
//            buf += mem_size;
//        } else {
//            // Throw exception if mem_size is large than UINT32_MAX
//            auto encode_size = streamvbyte_encode(
//                    reinterpret_cast<const uint32_t*>(col.get_null_map_data().data()),
//                    cast_set<UInt32>(upper_int32(mem_size)), (uint8_t*)(buf + sizeof(size_t)));
//            *reinterpret_cast<size_t*>(buf) = encode_size;
//            buf += (sizeof(size_t) + encode_size);
//        }
//        // data values
//        return nested_data_type->serialize(col.get_nested_column(), buf, be_exec_version);
//    }
        return nullptr;
}

const char* DataTypeLazy::deserialize(const char* buf, MutableColumnPtr* column,
                                          int be_exec_version) const {
//    if (be_exec_version >= USE_CONST_SERDE) {
//        auto* origin_column = column->get();
//        size_t real_have_saved_num = 0;
//        buf = deserialize_const_flag_and_row_num(buf, column, &real_have_saved_num);
//
//        auto* col = assert_cast<LazyColumn*>(origin_column);
//        // null flags
//        auto mem_size = real_have_saved_num * sizeof(bool);
//        col->get_null_map_data().resize(real_have_saved_num);
//        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
//            memcpy(col->get_null_map_data().data(), buf, mem_size);
//            buf += mem_size;
//        } else {
//            size_t encode_size = *reinterpret_cast<const size_t*>(buf);
//            buf += sizeof(size_t);
//            // Throw exception if mem_size is large than UINT32_MAX
//            streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(col->get_null_map_data().data()),
//                               cast_set<UInt32>(upper_int32(mem_size)));
//            buf += encode_size;
//        }
//        // column data values
//        auto nested = col->get_nested_column_ptr();
//        buf = nested_data_type->deserialize(buf, &nested, be_exec_version);
//        return buf;
//    } else {
//        auto* col = assert_cast<ColumnLazy*>(column->get());
//        // row num
//        uint32_t mem_size = *reinterpret_cast<const uint32_t*>(buf);
//        buf += sizeof(uint32_t);
//        // null flags
//        col->get_null_map_data().resize(mem_size / sizeof(bool));
//        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
//            memcpy(col->get_null_map_data().data(), buf, mem_size);
//            buf += mem_size;
//        } else {
//            size_t encode_size = *reinterpret_cast<const size_t*>(buf);
//            buf += sizeof(size_t);
//            // Throw exception if mem_size is large than UINT32_MAX
//            streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(col->get_null_map_data().data()),
//                               cast_set<UInt32>(upper_int32(mem_size)));
//            buf += encode_size;
//        }
//        // data values
//        auto nested = col->get_nested_column_ptr();
//        return nested_data_type->deserialize(buf, &nested, be_exec_version);
//    }
        return nullptr;
}

void DataTypeLazy::to_pb_column_meta(PColumnMeta* col_meta) const {
//    col_meta->set_is_lazy(true);
//    get_nested_type()->to_pb_column_meta(col_meta);
}

MutableColumnPtr DataTypeLazy::create_column() const {
   throw std::runtime_error("DataTypeLazy::create_column() should not be called");
}

Field DataTypeLazy::get_default() const {
    return Null();
}

bool DataTypeLazy::equals(const IDataType& rhs) const {
    return rhs.is_lazy() &&
           nested_data_type->equals(*static_cast<const DataTypeLazy&>(rhs).nested_data_type);
}

//DataTypePtr make_lazy(const DataTypePtr& type) {
//    if (type->is_lazy()) {
//        return type;
//    }
//    return std::make_shared<DataTypeLazy>(type);
//}

//DataTypes make_lazy(const DataTypes& types) {
//    DataTypes lazy_types;
//    for (const auto& type : types) {
//        lazy_types.push_back(make_lazy(type));
//    }
//    return lazy_types;
//}

DataTypePtr remove_lazy(const DataTypePtr& type) {
    if (type->is_lazy()) {
        return assert_cast<const DataTypeLazy*>(type.get())->get_nested_type();
    }
    return type;
}

DataTypes remove_lazy(const DataTypes& types) {
    DataTypes no_lazy_types;
    for (const auto& type : types) {
        no_lazy_types.push_back(remove_lazy(type));
    }
    return no_lazy_types;
}

bool have_lazy(const DataTypes& types) {
    return std::any_of(types.begin(), types.end(),
                       [](const DataTypePtr& type) { return type->is_lazy(); });
}

} // namespace doris::vectorized
