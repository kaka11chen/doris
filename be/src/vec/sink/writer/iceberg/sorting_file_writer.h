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

//#include "runtime/types.h"
//#include "vec/functions/function_string.h"
//#include "vec/data_types/data_type_factory.hpp"
//#include "util/bit_util.h"

#include <vector>

#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/common/sort/sorter.h"
#include "vec/core/block.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/sink/writer/iceberg/partition_writer.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris {

class RuntimeState;
class RuntimeProfile;

namespace iceberg {}; // namespace iceberg

namespace vectorized {
class Block;

//class PageSorter {
//class SortingBuffer {
//public:
//    SortingBuffer() {
//
//    }
////    void can_add(Block* block) {
////
////    }
//    Status add(Block* block)
//    {
//        RETURN_IF_ERROR(_sorter->append_block(block));
//        RETURN_IF_ERROR(_sorter->prepare_for_read());
//        size_t written_bytes = 0;
//        RETURN_IF_ERROR(writer_->write(state, block, written_bytes));
//        if (eof) {
//            RETURN_IF_ERROR(writer_->close());
//            writer_.reset();
//        }
//        return Status::OK();
////        usedMemoryBytes += page.getRetainedSizeInBytes();
////        rowCount = addExact(rowCount, page.getPositionCount());
//
//    }
//private:
////    std::vector<Block*> _blocks;
//    size_t _writer_sort_buffer_size;
//    std::unique_ptr<vectorized::Sorter> _sorter;
//
//};

template<typename OutputWriter>
class SortingFileWriter : public IPartitionWriter {
public:
    SortingFileWriter(const TDataSink& t_sink, std::vector<std::string> partition_values,
                      const VExprContextSPtrs& write_output_expr_ctxs,
                      const doris::iceberg::Schema& schema,
                      const std::string* iceberg_schema_json,
                      std::vector<std::string> write_column_names, WriteInfo write_info,
                      std::string file_name, int file_name_index,
                      TFileFormatType::type file_format_type,
                      TFileCompressType::type compress_type,
                      const std::map<std::string, std::string>& hadoop_conf)
//            : _partition_values(std::move(partition_values)),
                          :  _write_output_expr_ctxs(write_output_expr_ctxs),
              //              _non_write_columns_indices(non_write_columns_indices),
              //              _schema(schema),
              //              _iceberg_schema_json(iceberg_schema_json),
              _output_writer(t_sink, std::move(partition_values),
                           write_output_expr_ctxs,
                           schema,
                           iceberg_schema_json,
                           write_column_names,
                           std::move(write_info),
                             std::move(file_name),
                           file_name_index,
                           file_format_type,
                           compress_type,
                           hadoop_conf)
    //              _file_name_index(file_name_index),
    //              _file_format_type(file_format_type),
    //              _compress_type(compress_type),
    //              _hadoop_conf(hadoop_conf) {
    {}

    Status open(RuntimeState* state, RuntimeProfile* profile, const RowDescriptor* row_desc,
                ObjectPool* pool) override {
        DCHECK(row_desc);
        _state = state;
        _profile = profile;

        _spill_counters = ADD_LABEL_COUNTER_WITH_LEVEL(_profile, "Spill", 1);
        _spill_timer = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillTime", "Spill", 1);
        _spill_serialize_block_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillSerializeBlockTime", "Spill", 1);
        _spill_write_disk_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillWriteDiskTime", "Spill", 1);
        _spill_data_size = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "SpillWriteDataSize",
                                                        TUnit::BYTES, "Spill", 1);
        _spill_block_count = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "SpillWriteBlockCount",
                                                          TUnit::UNIT, "Spill", 1);
        _spill_wait_in_queue_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillWaitInQueueTime", "Spill", 1);
        _spill_write_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillWriteWaitIOTime", "Spill", 1);
        _spill_read_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillReadWaitIOTime", "Spill", 1);

        _spill_recover_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillRecoverTime", "Spill", 1);
        _spill_read_data_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillReadDataTime", "Spill", 1);
        _spill_deserialize_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillDeserializeTime", "Spill", 1);
        _spill_read_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "SpillReadDataSize",
                                                         TUnit::BYTES, "Spill", 1);
        _spill_read_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillReadWaitIOTime", "Spill", 1);

        int limit = -1;
        int64_t offset = 0;
//        std::vector<bool> is_asc_order = {true};
//        std::vector<bool> nulls_first = {true};
//        VSortExecExprs vsort_exec_exprs;
//        VExprContextSPtrs lhs_ordering_expr_ctxs;
//        VExprContextSPtrs rhs_ordering_expr_ctxs;

        VExprSPtr ordering_slot1 = _write_output_expr_ctxs[1]->root();
        VExprContextSPtr lhs_ordering_expr_ctx1 = VExprContext::create_shared(ordering_slot1);
        RETURN_IF_ERROR(lhs_ordering_expr_ctx1->prepare(_state, *row_desc));
        RETURN_IF_ERROR(lhs_ordering_expr_ctx1->open(_state));
        _lhs_ordering_expr_ctxs.emplace_back(lhs_ordering_expr_ctx1);
        fprintf(stderr, "000 lhs_ordering_expr_ctxs.size(): %ld\n", _lhs_ordering_expr_ctxs.size());

//        Status VSortExecExprs::init(const std::vector<TExpr>& ordering_exprs,
//                                    const std::vector<TExpr>* sort_tuple_slot_exprs, ObjectPool* pool)

        RETURN_IF_ERROR(_vsort_exec_exprs.init(_lhs_ordering_expr_ctxs, _rhs_ordering_expr_ctxs));

        fprintf(stderr, "111 vsort_exec_exprs.lhs_ordering_expr_ctxs().size(): %ld, _vsort_exec_exprs.need_materialize_tuple(): %d\n", _vsort_exec_exprs.lhs_ordering_expr_ctxs().size(), _vsort_exec_exprs.need_materialize_tuple());
        RowDescriptor output_row_desc;
        RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, *row_desc, output_row_desc));
        RETURN_IF_ERROR(_vsort_exec_exprs.open(state));

        fprintf(stderr, "222 vsort_exec_exprs.lhs_ordering_expr_ctxs().size(): %ld, _vsort_exec_exprs.need_materialize_tuple(): %d\n", _vsort_exec_exprs.lhs_ordering_expr_ctxs().size(), _vsort_exec_exprs.need_materialize_tuple());

        _sorter = vectorized::FullSorter::create_unique(_vsort_exec_exprs, limit, offset, pool,
                                                        _is_asc_order, _nulls_first, *row_desc, state,
                                                        _profile);
        _sorter->set_enable_spill(true);

        _merger = std::make_unique<vectorized::VSortedRunMerger>(
                _sorter->get_sort_description(), SPILL_BLOCK_BATCH_ROW_COUNT, limit, offset, _profile);
        RETURN_IF_ERROR(_output_writer.open(state, profile, row_desc, pool));
        return Status::OK();
    }

    Status write(vectorized::Block& block) override {
        RETURN_IF_ERROR(_sorter->append_block(&block));

        if (_sorter->state().get_sorted_block().empty()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(flush_to_temp_file());
        return Status::OK();
    }

    Status flush_to_temp_file() {
        fprintf(stderr, "flush_to_temp_file\n");
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                _state, _spilling_stream, print_id(_state->query_id()), "sort", 1 /* node_id */,
                SPILL_BLOCK_BATCH_ROW_COUNT, SORT_BLOCK_SPILL_BATCH_BYTES, _profile));

        _sorted_streams.emplace_back(_spilling_stream);

        _spilling_stream->set_write_counters(_spill_serialize_block_timer,
                                             _spill_block_count, _spill_data_size,
                                             _spill_write_disk_timer,
                                             _spill_write_wait_io_timer);

        RETURN_IF_ERROR(_spilling_stream->prepare_spill());

        //        std::vector<Block>& sorted_blocks = _sorter->state().get_sorted_block();
        RETURN_IF_ERROR(_sorter->prepare_for_read());
        bool eos = false;
        Block block;
        while (!eos) {
            RETURN_IF_ERROR(_sorter->merge_sort_read_for_spill(_state, &block, 4096, &eos));
            RETURN_IF_ERROR(_spilling_stream->spill_block(_state, block, eos));
            block.clear_column_data();
        }
        //        tempFiles.add(new TempFile(tempFile, writer.getWrittenBytes()));
        _sorter->reset();
        return Status::OK();
    }

    Status close(const Status& status) override {
        RETURN_IF_ERROR(_combine_files());
        bool eos = false;
        while (!eos) {
            Block output_block;
            RETURN_IF_ERROR(_merger->get_next(&output_block, &eos));
            fprintf(stderr, "output_block.rows(): %ld\n", output_block.rows());
            RETURN_IF_ERROR(_output_writer.write(output_block));
        }
        RETURN_IF_ERROR(_output_writer.close(status));
        return Status::OK();
    }

    inline const std::string& file_name() const override { return _output_writer.file_name(); }

    inline int file_name_index() const override { return _output_writer.file_name_index(); }

    inline size_t written_len() override { return _output_writer.written_len(); }

private:
    Status _combine_files() {
        fprintf(stderr, "_combine_files\n");
        Block merge_sorted_block;
        SpillStreamSPtr tmp_stream;
        while (true) {
            int max_stream_count = 2;
            {
                //                SCOPED_TIMER(Base::_spill_recover_time);
                RETURN_IF_ERROR(_create_intermediate_merger(max_stream_count, _sorter->get_sort_description()));
            }
            //            RETURN_IF_ERROR(_status);

            // all the remaining streams can be merged in a run
            if (_sorted_streams.empty()) {
                return Status::OK();
            }

            {
                RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        _state, tmp_stream, print_id(_state->query_id()), "sort", 1 /* node_id */,
                        SPILL_BLOCK_BATCH_ROW_COUNT, SORT_BLOCK_SPILL_BATCH_BYTES, _profile));

                //                RETURN_IF_ERROR(_status);
                RETURN_IF_ERROR(tmp_stream->prepare_spill());
                //                RETURN_IF_ERROR(_status);

                _sorted_streams.emplace_back(tmp_stream);

                bool eos = false;
                                tmp_stream->set_write_counters(_spill_serialize_block_timer, _spill_block_count,
                                                               _spill_data_size, _spill_write_disk_timer,
                                                               _spill_write_wait_io_timer);
                while (!eos && !_state->is_cancelled()) {
                    merge_sorted_block.clear_column_data();
                    {
                        //                        SCOPED_TIMER(Base::_spill_recover_time);
                        RETURN_IF_ERROR(_merger->get_next(&merge_sorted_block, &eos));
                    }
                    RETURN_IF_ERROR(tmp_stream->spill_block(_state, merge_sorted_block, eos));
                    fprintf(stderr, "tmp_stream->spill_block(_state, merge_sorted_block, eos)\n");
                }
            }
            for (auto& stream : _current_merging_streams) {
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            _current_merging_streams.clear();
        }
        return Status::OK();
    }

    Status _create_intermediate_merger(int num_blocks,
                                       const vectorized::SortDescription& sort_description) {
        int limit = -1;
        int64_t offset = 0;
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        _merger = std::make_unique<vectorized::VSortedRunMerger>(
                sort_description, SPILL_BLOCK_BATCH_ROW_COUNT, limit, offset, _profile);

        _current_merging_streams.clear();
        for (int i = 0; i < num_blocks && !_sorted_streams.empty(); ++i) {
            auto stream = _sorted_streams.front();
                        stream->set_read_counters(_spill_read_data_time, _spill_deserialize_time,
                                                  _spill_read_bytes, _spill_read_wait_io_timer);
            _current_merging_streams.emplace_back(stream);
            child_block_suppliers.emplace_back(
                    std::bind(std::mem_fn(&vectorized::SpillStream::read_next_block_sync),
                              stream.get(), std::placeholders::_1, std::placeholders::_2));

            _sorted_streams.pop_front();
        }
        RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
        return Status::OK();
    }

    std::vector<bool> _is_asc_order = {true};
    std::vector<bool> _nulls_first = {true};
    VSortExecExprs _vsort_exec_exprs;
    VExprContextSPtrs _lhs_ordering_expr_ctxs;
    VExprContextSPtrs _rhs_ordering_expr_ctxs;

    std::unique_ptr<vectorized::FullSorter> _sorter;
    std::unique_ptr<vectorized::VSortedRunMerger> _merger;

    vectorized::SpillStreamSPtr _spilling_stream;
    std::deque<vectorized::SpillStreamSPtr> _sorted_streams;

    std::string _path;

    std::vector<std::string> _partition_values;

    //    size_t _row_count = 0;

//    const VExprContextSPtrs& _vec_output_expr_ctxs;
        const VExprContextSPtrs& _write_output_expr_ctxs;
    //    const std::set<size_t>& _non_write_columns_indices;

    //    const doris::iceberg::Schema& _schema;
    //    const std::string* _iceberg_schema_json;
//    IPartitionWriter::WriteInfo _write_info;
//    std::string _file_name;
    //    int _file_name_index;
    //    TFileFormatType::type _file_format_type;
    //    TFileCompressType::type _compress_type;
    //    const std::map<std::string, std::string>& _hadoop_conf;

//    std::shared_ptr<io::FileSystem> _fs = nullptr;
//
//    // If the result file format is plain text, like CSV, this _file_writer is owned by this FileResultWriter.
//    // If the result file format is Parquet, this _file_writer is owned by _parquet_writer.
//    std::unique_ptr<doris::io::FileWriter> _file_writer = nullptr;
//    // convert block to parquet/orc/csv format
//    std::unique_ptr<VFileFormatTransformer> _file_format_transformer = nullptr;

    RuntimeState* _state;
    RuntimeProfile* _profile;

    std::vector<vectorized::SpillStreamSPtr> _current_merging_streams;

    OutputWriter _output_writer;

    RuntimeProfile::Counter* _spill_counters = nullptr;
    RuntimeProfile::Counter* _spill_timer = nullptr;
    RuntimeProfile::Counter* _spill_serialize_block_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_disk_timer = nullptr;
    RuntimeProfile::Counter* _spill_data_size = nullptr;
    RuntimeProfile::Counter* _spill_block_count = nullptr;
    RuntimeProfile::Counter* _spill_wait_in_queue_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_wait_io_timer = nullptr;

    RuntimeProfile::Counter* _spill_recover_time;
    RuntimeProfile::Counter* _spill_read_data_time;
    RuntimeProfile::Counter* _spill_deserialize_time;
    RuntimeProfile::Counter* _spill_read_bytes;
    RuntimeProfile::Counter* _spill_read_wait_io_timer = nullptr;

    static constexpr int SORT_BLOCK_SPILL_BATCH_BYTES = 8 * 1024 * 1024;
    static constexpr size_t SPILL_BLOCK_BATCH_ROW_COUNT = 4096;
};

} // namespace vectorized
} // namespace doris