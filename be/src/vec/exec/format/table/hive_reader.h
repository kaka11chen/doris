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
#include <memory>
#include <vector>

#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader2.h"
#include "vec/exec/format/table/table_format_reader.h"
namespace doris::vectorized {
#include "common/compile_check_begin.h"

class ColumnAdaptation;

class HiveReader : public TableFormatReader, public TableSchemaChangeHelper {
public:
    HiveReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
               RuntimeState* state, const TFileScanRangeParams& params, const TFileRangeDesc& range,
               io::IOContext* io_ctx)
            : TableFormatReader(std::move(file_format_reader), state, profile, params, range,
                                io_ctx) {};

    ~HiveReader() override = default;

    Status get_file_col_id_to_name(bool& exist_schema,
                                   std::map<int, std::string>& file_col_id_to_name) final;

    Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) final;

    Status init_row_filters() final { return Status::OK(); };

protected:
    std::vector<std::shared_ptr<ColumnAdaptation>> _column_adaptations;
};

class HiveOrcReader final : public HiveReader {
public:
    ENABLE_FACTORY_CREATOR(HiveOrcReader);
    HiveOrcReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                  RuntimeState* state, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, io::IOContext* io_ctx)
            : HiveReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~HiveOrcReader() final = default;

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            const VExprContextSPtrs* conjuncts);
};

class HiveParquetReader final : public HiveReader {
public:
    ENABLE_FACTORY_CREATOR(HiveParquetReader);
    HiveParquetReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                      RuntimeState* state, const TFileScanRangeParams& params,
                      const TFileRangeDesc& range, io::IOContext* io_ctx)
            : HiveReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~HiveParquetReader() final = default;

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            const VExprContextSPtrs* conjuncts);

private:
    // Helper function to compare strings case-insensitively
    bool case_insensitive_compare(const std::string& str1, const std::string& str2) {
        if (str1.size() != str2.size()) {
            return false;
        }
        return std::equal(str1.begin(), str1.end(), str2.begin(), [](char a, char b) {
            return std::tolower(a) == std::tolower(b);
        });
    }

    // Lookup a column by name in Parquet metadata, hms column name is lowercase.
    const tparquet::SchemaElement* lookup_column_by_name(const tparquet::FileMetaData* file_meta_data, const std::string& column_name) {
        if (!file_meta_data || file_meta_data->schema.empty()) {
            return nullptr;
        }

        for (const auto& schema_element : file_meta_data->schema) {
            if (case_insensitive_compare(schema_element.name, column_name)) {
                return &schema_element;
            }
        }

        // If no match is found, return nullptr
        return nullptr;
    }
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized