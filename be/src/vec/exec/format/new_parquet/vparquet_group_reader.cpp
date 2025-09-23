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

#include "vparquet_group_reader.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <string.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "schema_desc.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vparquet_column_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RuntimeState;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::vectorized::new_parquet {
#include "common/compile_check_begin.h"

RowGroupReader::RowGroupReader(io::FileReaderSPtr file_reader,
                               const std::vector<std::string>& table_column_names,
                               const int32_t row_group_id, const tparquet::RowGroup& row_group,
                               cctz::time_zone* ctz, io::IOContext* io_ctx, RuntimeState* state,
                               converter::ColumnTypeConverterFactory* converter_factory)
        : _file_reader(file_reader),
          _table_column_names(table_column_names),
          _row_group_id(row_group_id),
          _row_group_meta(row_group),
          _remaining_rows(row_group.num_rows),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _state(state),
          _obj_pool(new ObjectPool()),
          _converter_factory(converter_factory) {}

RowGroupReader::~RowGroupReader() {
    _column_readers.clear();
    _obj_pool->clear();
}

Status RowGroupReader::init(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
                            std::unordered_map<int, tparquet::OffsetIndex>& col_offsets) {
    _merge_read_ranges(row_ranges);
    if (_table_column_names.empty()) {
        // Query task that only select columns in path.
        return Status::OK();
    }
    const size_t MAX_GROUP_BUF_SIZE = config::parquet_rowgroup_max_buffer_mb << 20;
    const size_t MAX_COLUMN_BUF_SIZE = config::parquet_column_max_buffer_mb << 20;
    size_t max_buf_size =
            std::min(MAX_COLUMN_BUF_SIZE, MAX_GROUP_BUF_SIZE / _table_column_names.size());
    for (const auto& table_column_name : _table_column_names) {
        auto read_file_col = _table_info_node_ptr->children_file_column_name(table_column_name);

        auto* field = const_cast<FieldSchema*>(schema.get_column(read_file_col));
        auto physical_index = field->physical_column_index;
        std::unique_ptr<ParquetColumnReader> reader;
        // TODO : support rested column types
        const tparquet::OffsetIndex* offset_index =
                col_offsets.find(physical_index) != col_offsets.end() ? &col_offsets[physical_index]
                                                                      : nullptr;
        RETURN_IF_ERROR(ParquetColumnReader::create(_file_reader, field, _row_group_meta,
                                                    _read_ranges, _ctz, _io_ctx, reader,
                                                    max_buf_size, offset_index));
        if (reader == nullptr) {
            VLOG_DEBUG << "Init row group(" << _row_group_id << ") reader failed";
            return Status::Corruption("Init row group reader failed");
        }
        _column_readers[table_column_name] = std::move(reader);
    }
    return Status::OK();
}

Status RowGroupReader::next_batch(Block* block, size_t batch_size, size_t* read_rows,
                                  bool* batch_eof) {
    if (_is_row_group_filtered) {
        *read_rows = 0;
        *batch_eof = true;
        return Status::OK();
    }

    // Process external table query task that select columns are all from path.
    if (_table_column_names.empty()) {
        RETURN_IF_ERROR(_read_empty_batch(batch_size, read_rows, batch_eof));

        // RETURN_IF_ERROR(_fill_row_id_columns(block, *read_rows, modify_row_ids));
        *read_rows = block->rows();
        return Status::OK();
    }
    FilterMap filter_map;
    RETURN_IF_ERROR(_read_column_data(block, _table_column_names, batch_size, read_rows, batch_eof,
                                      filter_map));
    *read_rows = block->rows();
    return Status::OK();
}

void RowGroupReader::_merge_read_ranges(std::vector<RowRange>& row_ranges) {
    _read_ranges = row_ranges;
    _remaining_rows = 0;
    for (auto& range : row_ranges) {
        _remaining_rows += range.last_row - range.first_row;
    }
}

Status RowGroupReader::_read_column_data(Block* block,
                                         const std::vector<std::string>& table_column_names,
                                         size_t batch_size, size_t* read_rows, bool* batch_eof,
                                         FilterMap& filter_map) {
    size_t batch_read_rows = 0;
    bool has_eof = false;
    for (auto& table_col_name : table_column_names) {
        auto& column_with_type_and_name = block->get_by_name(table_col_name);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;

        size_t col_read_rows = 0;
        bool col_eof = false;
        // Should reset _filter_map_index to 0 when reading next column.
        _column_readers[table_col_name]->reset_filter_map_index();
        while (!col_eof && col_read_rows < batch_size) {
            size_t loop_rows = 0;
            RETURN_IF_ERROR(_column_readers[table_col_name]->read_column_data(
                    column_ptr, column_type,
                    _table_info_node_ptr->get_children_node(table_col_name), filter_map,
                    batch_size - col_read_rows, &loop_rows, &col_eof, false, _converter_factory));
            col_read_rows += loop_rows;
        }
        if (batch_read_rows > 0 && batch_read_rows != col_read_rows) {
            return Status::Corruption("Can't read the same number of rows among parquet columns");
        }
        batch_read_rows = col_read_rows;
        if (col_eof) {
            has_eof = true;
        }
    }

    *read_rows = batch_read_rows;
    *batch_eof = has_eof;

    return Status::OK();
}

Status RowGroupReader::_read_empty_batch(size_t batch_size, size_t* read_rows, bool* batch_eof) {
    if (batch_size < _remaining_rows) {
        *read_rows = batch_size;
        _remaining_rows -= batch_size;
        *batch_eof = false;
    } else {
        *read_rows = _remaining_rows;
        _remaining_rows = 0;
        *batch_eof = true;
    }
    _total_read_rows += *read_rows;
    return Status::OK();
}

Status RowGroupReader::_get_current_batch_row_id(size_t read_rows) {
    _current_batch_row_ids.clear();
    _current_batch_row_ids.resize(read_rows);

    int64_t idx = 0;
    int64_t read_range_rows = 0;
    for (auto& range : _read_ranges) {
        if (read_rows == 0) {
            break;
        }
        if (read_range_rows + (range.last_row - range.first_row) > _total_read_rows) {
            int64_t fi =
                    std::max(_total_read_rows, read_range_rows) - read_range_rows + range.first_row;
            size_t len = std::min(read_rows, (size_t)(std::max(range.last_row, fi) - fi));

            read_rows -= len;

            for (auto i = 0; i < len; i++) {
                _current_batch_row_ids[idx++] =
                        (rowid_t)(fi + i + _current_row_group_idx.first_row);
            }
        }
        read_range_rows += range.last_row - range.first_row;
    }
    return Status::OK();
}

ParquetColumnReader::Statistics RowGroupReader::statistics() {
    ParquetColumnReader::Statistics st;
    for (auto& reader : _column_readers) {
        auto ost = reader.second->statistics();
        st.merge(ost);
    }
    return st;
}
#include "common/compile_check_end.h"

} // namespace doris::vectorized::new_parquet
