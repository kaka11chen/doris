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

#include "vparquet_group_reader2.h"

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
#include "gutil/stringprintf.h"
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
#include "vec/columns/columns_number.h"
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

#include "vec/columns/dictionary_column.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RuntimeState;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
const std::vector<int64_t> RowGroupReader2::NO_DELETE = {};

RowGroupReader2::RowGroupReader2(io::FileReaderSPtr file_reader,
                               const std::vector<std::string>& read_columns,
                               const int32_t row_group_id, const tparquet::RowGroup& row_group,
                               cctz::time_zone* ctz, io::IOContext* io_ctx, RuntimeState* state)
        : _file_reader(file_reader),
          _read_columns(read_columns),
          _row_group_id(row_group_id),
          _row_group_meta(row_group),
          _remaining_rows(row_group.num_rows),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _state(state),
          _obj_pool(new ObjectPool()) {}

RowGroupReader2::~RowGroupReader2() {
    _column_readers.clear();
    _obj_pool->clear();
}

Status RowGroupReader2::init(
        const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
        std::unordered_map<int, tparquet::OffsetIndex>& col_offsets) {
//    _tuple_descriptor = tuple_descriptor;
//    _row_descriptor = row_descriptor;
//    _col_name_to_slot_id = colname_to_slot_id;
    _merge_read_ranges(row_ranges);
    if (_read_columns.empty()) {
        // Query task that only select columns in path.
        return Status::OK();
    }
    const size_t MAX_GROUP_BUF_SIZE = config::parquet_rowgroup_max_buffer_mb << 20;
    const size_t MAX_COLUMN_BUF_SIZE = config::parquet_column_max_buffer_mb << 20;
    size_t max_buf_size = std::min(MAX_COLUMN_BUF_SIZE, MAX_GROUP_BUF_SIZE / _read_columns.size());
    for (const auto& read_col : _read_columns) {
        auto* field = const_cast<FieldSchema*>(schema.get_column(read_col));
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
        _column_readers[read_col] = std::move(reader);

//        if (_can_filter_by_dict(_row_group_meta.columns[field->physical_column_index].meta_data)) {
//            _dict_col_names.emplace_back(read_col);
//        }
    }

//    for (const auto& dict_col_name : _dict_col_names) {
//        // 1. Get dictionary values to a string column.
//        MutableColumnPtr dict_value_column = ColumnString::create();
//        bool has_dict = false;
//        RETURN_IF_ERROR(_column_readers[dict_col_name]->read_dict_values_to_column(
//                dict_value_column, &has_dict));
//        DCHECK(has_dict);
//        _column_readers[dict_col_name]->set_dictionary(std::move(dict_value_column));
//    }
    return Status::OK();
}

bool RowGroupReader2::_can_filter_by_dict(const tparquet::ColumnMetaData& column_metadata) {
    if (column_metadata.type != tparquet::Type::BYTE_ARRAY) {
        return false;
    }

    if (!_is_dictionary_encoded(column_metadata)) {
        return false;
    }

//    std::function<bool(const VExpr* expr)> visit_function_call = [&](const VExpr* expr) {
//        // TODO: The current implementation of dictionary filtering does not take into account
//        //  the implementation of NULL values because the dictionary itself does not contain
//        //  NULL value encoding. As a result, many NULL-related functions or expressions
//        //  cannot work properly, such as is null, is not null, coalesce, etc.
//        //  Here we first disable dictionary filtering when predicate is not slot.
//        //  Implementation of NULL value dictionary filtering will be carried out later.
//        if (expr->node_type() != TExprNodeType::SLOT_REF) {
//            return false;
//        }
//        for (auto& child : expr->children()) {
//            if (!visit_function_call(child.get())) {
//                return false;
//            }
//        }
//        return true;
//    };
//    for (auto& ctx : _slot_id_to_filter_conjuncts->at(slot_id)) {
//        if (!visit_function_call(ctx->root().get())) {
//            return false;
//        }
//    }
    return true;
}

// This function is copied from
// https://github.com/apache/impala/blob/master/be/src/exec/parquet/hdfs-parquet-scanner.cc#L1717
bool RowGroupReader2::_is_dictionary_encoded(const tparquet::ColumnMetaData& column_metadata) {
    // The Parquet spec allows for column chunks to have mixed encodings
    // where some data pages are dictionary-encoded and others are plain
    // encoded. For example, a Parquet file writer might start writing
    // a column chunk as dictionary encoded, but it will switch to plain
    // encoding if the dictionary grows too large.
    //
    // In order for dictionary filters to skip the entire row group,
    // the conjuncts must be evaluated on column chunks that are entirely
    // encoded with the dictionary encoding. There are two checks
    // available to verify this:
    // 1. The encoding_stats field on the column chunk metadata provides
    //    information about the number of data pages written in each
    //    format. This allows for a specific check of whether all the
    //    data pages are dictionary encoded.
    // 2. The encodings field on the column chunk metadata lists the
    //    encodings used. If this list contains the dictionary encoding
    //    and does not include unexpected encodings (i.e. encodings not
    //    associated with definition/repetition levels), then it is entirely
    //    dictionary encoded.
    if (column_metadata.__isset.encoding_stats) {
        // Condition #1 above
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if (enc_stat.page_type == tparquet::PageType::DATA_PAGE &&
                (enc_stat.encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                 enc_stat.encoding != tparquet::Encoding::RLE_DICTIONARY) &&
                enc_stat.count > 0) {
                return false;
            }
        }
    } else {
        // Condition #2 above
        bool has_dict_encoding = false;
        bool has_nondict_encoding = false;
        for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
            if (encoding == tparquet::Encoding::PLAIN_DICTIONARY ||
                encoding == tparquet::Encoding::RLE_DICTIONARY) {
                has_dict_encoding = true;
            }

            // RLE and BIT_PACKED are used for repetition/definition levels
            if (encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                encoding != tparquet::Encoding::RLE_DICTIONARY &&
                encoding != tparquet::Encoding::RLE && encoding != tparquet::Encoding::BIT_PACKED) {
                has_nondict_encoding = true;
                break;
            }
        }
        // Not entirely dictionary encoded if:
        // 1. No dictionary encoding listed
        // OR
        // 2. Some non-dictionary encoding is listed
        if (!has_dict_encoding || has_nondict_encoding) {
            return false;
        }
    }

    return true;
}

Status RowGroupReader2::next_batch(Block* block, size_t batch_size, size_t* read_rows,
                                  bool* batch_eof) {
    if (_is_row_group_filtered) {
        *read_rows = 0;
        *batch_eof = true;
        return Status::OK();
    }

    // Process external table query task that select columns are all from path.
//    if (_read_columns.empty()) {
//        RETURN_IF_ERROR(_read_empty_batch(batch_size, read_rows, batch_eof));
//        RETURN_IF_ERROR(
//                _fill_partition_columns(block, *read_rows, _lazy_read_ctx.partition_columns));
//        RETURN_IF_ERROR(_fill_missing_columns(block, *read_rows, _lazy_read_ctx.missing_columns));
//
//        Status st = VExprContext::filter_block(_lazy_read_ctx.conjuncts, block, block->columns());
//        *read_rows = block->rows();
//        return st;
//    }
        FilterMap filter_map;
        RETURN_IF_ERROR(_read_columns_data(block, _read_columns, batch_size,
                                          read_rows, batch_eof, filter_map));

        *read_rows = block->rows();
        return Status::OK();
//    }
}

void RowGroupReader2::_merge_read_ranges(std::vector<RowRange>& row_ranges) {
    _read_ranges = row_ranges;
}

Status RowGroupReader2::_read_columns_data(Block* block, const std::vector<std::string>& columns,
                                         size_t batch_size, size_t* read_rows, bool* batch_eof,
                                         FilterMap& filter_map) {
    size_t batch_read_rows = 0;
    bool has_eof = false;
    for (auto& read_col_name : columns) {
        auto& column_with_type_and_name = block->get_by_name(read_col_name);
//        auto& column_ptr = column_with_type_and_name.column;
//        auto& column_type = column_with_type_and_name.type;
//        bool is_dict_filter = false;
//        for (auto& _dict_col_name : _dict_col_names) {
//            if (_dict_col_name == read_col_name) {
//                size_t pos = block->get_position_by_name(read_col_name);
//                if (column_type->is_nullable()) {
//                    block->get_by_position(pos).type =
//                            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDictionary>());
//                    block->get_by_position(pos).column = ColumnNullable::create(std::move(dict_column),
//                                                                                ColumnUInt8::create(dict_column->size(), 0));
//                } else {
//                    block->get_by_position(pos).type = std::make_shared<DataTypeDictionary>();
//                    block->get_by_position(pos).column = std::move(dict_column);
//                }
//                is_dict_filter = true;
//                break;
//            }
//        }

        size_t col_read_rows = 0;
        bool col_eof = false;
        RETURN_IF_ERROR(read_column_data(&column_with_type_and_name, batch_size, &col_read_rows, &col_eof, filter_map));
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

Status RowGroupReader2::read_column_data(ColumnWithTypeAndName* column_with_type_and_name,
                                           size_t batch_size, size_t* read_rows, bool* eof,
                                           FilterMap& filter_map) {
    auto& column_ptr = column_with_type_and_name->column;
    auto& column_type = column_with_type_and_name->type;

    size_t col_read_rows = 0;
    bool col_eof = false;
    _column_readers[column_with_type_and_name->name]->reset_filter_map_index();
    while (!col_eof && col_read_rows < batch_size) {
        size_t loop_rows = 0;
        RETURN_IF_ERROR(_column_readers[column_with_type_and_name->name]->read_column_data(
                column_ptr, column_type, filter_map, batch_size - col_read_rows, &loop_rows,
                &col_eof, false));
        col_read_rows += loop_rows;
    }
    *read_rows = col_read_rows;
    *eof = col_eof;
    return Status::OK();
}

ParquetColumnReader::Statistics RowGroupReader2::statistics() {
    ParquetColumnReader::Statistics st;
    for (auto& reader : _column_readers) {
        auto ost = reader.second->statistics();
        st.merge(ost);
    }
    return st;
}
#include "common/compile_check_end.h"

} // namespace doris::vectorized
