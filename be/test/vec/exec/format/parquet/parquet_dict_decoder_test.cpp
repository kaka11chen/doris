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

#include <gtest/gtest.h>

#include "vec/common/assert_cast.h"
#include "vec/common/custom_allocator.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/exec/format/parquet/byte_array_dict_decoder.h"
#include "vec/exec/format/parquet/fix_length_dict_decoder.hpp"

namespace doris::vectorized {

TEST(ParquetDictDecoderTest, ByteArrayEmptyDictConvert) {
    ByteArrayDictDecoder decoder;
    auto dict = make_unique_buffer<uint8_t>(0);
    ASSERT_TRUE(decoder.set_dict(dict, 0, 0).ok());

    auto dict_values = ColumnString::create();
    ASSERT_TRUE(decoder.read_dict_values_to_column(dict_values).ok());
    EXPECT_EQ(dict_values->size(), 0);

    auto dict_column = ColumnInt32::create();
    dict_column->insert_many_defaults(3);
    auto converted = decoder.convert_dict_column_to_string_column(
            assert_cast<const ColumnInt32*>(dict_column.get()));
    ASSERT_EQ(converted->size(), 3);
    const auto& string_column = assert_cast<const ColumnString&>(*converted);
    for (size_t i = 0; i < string_column.size(); ++i) {
        EXPECT_EQ(string_column.get_data_at(i).size, 0U);
    }
}

TEST(ParquetDictDecoderTest, FixLengthEmptyDictConvert) {
    FixLengthDictDecoder<tparquet::Type::INT32> decoder;
    decoder.set_type_length(sizeof(int32_t));
    auto dict = make_unique_buffer<uint8_t>(0);
    ASSERT_TRUE(decoder.set_dict(dict, 0, 0).ok());

    auto dict_values = ColumnString::create();
    ASSERT_TRUE(decoder.read_dict_values_to_column(dict_values).ok());
    EXPECT_EQ(dict_values->size(), 0);

    auto dict_column = ColumnInt32::create();
    dict_column->insert_many_defaults(2);
    auto converted = decoder.convert_dict_column_to_string_column(
            assert_cast<const ColumnInt32*>(dict_column.get()));
    ASSERT_EQ(converted->size(), 2);
    const auto& string_column = assert_cast<const ColumnString&>(*converted);
    for (size_t i = 0; i < string_column.size(); ++i) {
        EXPECT_EQ(string_column.get_data_at(i).size, 0U);
    }
}

} // namespace doris::vectorized
