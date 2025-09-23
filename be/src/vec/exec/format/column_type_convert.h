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

#include <absl/strings/numbers.h>
#include <cctz/time_zone.h>

#include <cstdint>
#include <utility>

#include "util/to_string.h"
#include "vec/columns/column_string.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized::converter {
#include "common/compile_check_begin.h"

// Forward declarations and enums
class ColumnTypeConverter;
enum FileFormat { COMMON, ORC, PARQUET };

/**
 * Factory interface for creating column type converters
 */
class ColumnTypeConverterFactory {
public:
    virtual ~ColumnTypeConverterFactory() = default;

    /**
     * Create a converter to change column type
     * @param src_type column type from file meta data
     * @param dst_type column type from FE planner(the changed column type)
     * @param file_format the file format being processed
     */
    virtual std::unique_ptr<ColumnTypeConverter> create_converter(const DataTypePtr& src_type,
                                                                  const DataTypePtr& dst_type,
                                                                  FileFormat file_format) = 0;
};

/**
 * Unified schema change interface for all format readers:
 *
 * First, read the data according to the column type of the file into source column
 * Second, convert source column to the destination column with type planned by FE
 */
class ColumnTypeConverter {
protected:
    // The cached column to read data according to the column type of the file
    // Then, it will be converted to destination column, so this column can be reuse in next loop
    ColumnPtr _cached_src_column = nullptr;
    // The column type generated from file meta(eg. parquet footer)
    DataTypePtr _cached_src_type = nullptr;
    // Error message to show unsupported converter if support() return false;
    std::string _error_msg;

public:
    /**
     * Get the converter using specified factory
     * @param src_type column type from file meta data
     * @param dst_type column type from FE planner(the changed column type)
     * @param file_format the file format being processed
     */
    static std::unique_ptr<ColumnTypeConverter> get_converter(const DataTypePtr& src_type,
                                                              const DataTypePtr& dst_type,
                                                              FileFormat file_format);

    ColumnTypeConverter() = default;
    virtual ~ColumnTypeConverter() = default;

    /**
     * Converter source column to destination column. If the converter is not consistent,
     * the source column is `_cached_src_column`, otherwise, `src_col` and `dst_col` are the
     * same column, and with nothing to do.
     */
    virtual Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) { return Status::OK(); }

    virtual bool support() { return true; }

    virtual bool is_consistent() { return false; }

    /**
     * Get the column to read data from file with the type from file meta data.
     * If the converter is not consistent, the returned column is `_cached_src_column`.
     * For performance reasons, the null map of `_cached_src_column` is a reference from
     * the null map of `dst_column`, so there is no need to convert null map in `convert()`.
     *
     * According to the hive standard, if certain values fail to be converted(eg. string `row1` to int value),
     * these values are replaced by nulls.
     */
    ColumnPtr get_column(const DataTypePtr& src_type, ColumnPtr& dst_column,
                         const DataTypePtr& dst_type);

    /**
     * Get the column type from file meta data.
     */
    const DataTypePtr& get_type() { return _cached_src_type; }

    std::string get_error_msg() { return _error_msg; };
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized::converter
