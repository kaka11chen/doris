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

#include "blackhole_sink_operator.h"

#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <sstream>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "util/mysql_row_buffer.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

BlackholeSinkOperatorX::BlackholeSinkOperatorX(
        int operator_id, const int dest_id, const TDataStreamSink& sink,
        const std::vector<TPlanFragmentDestination>& destinations)
        : Base(operator_id, 0, dest_id), _t_data_stream_sink(sink), _destinations(destinations) {
    LOG(INFO) << "BlackholeSinkOperatorX created, operator_id: " << operator_id;
}

Status BlackholeSinkOperatorX::prepare(RuntimeState* state) {
    // BlackholeSink uses result buffer to send cache metrics to FE
    // Similar to VArrowFlightResultWriter pattern
    // if (state->query_options().enable_parallel_result_sink) {
    //     VLOG_DEBUG << "create sender in prepare with query id " << state->query_id();
    //     std::shared_ptr<ResultBlockBufferBase> sender_base = nullptr;
    //     RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
    //             state->query_id(), 4096, &sender_base, state,
    //             false, nullptr));
    //     _sender = sender_base;
    //     LOG(INFO) << "Created shared sender in prepare for blackhole sink, query_id: " << print_id(state->query_id());
    // }
    return Status::OK();
}

Status BlackholeSinkOperatorX::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSinkOperatorXBase::init(tsink));
    // BlackholeSink doesn't need complex initialization like regular sinks
    return Status::OK();
}

Status BlackholeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)block->rows());

    LOG(INFO) << "BlackholeSink::sink called, rows: " << (block ? block->rows() : 0)
              << ", eos: " << (eos ? "true" : "false");

    if (block && block->rows() > 0) {
        // Process the block (essentially discard it but collect metrics)
        RETURN_IF_ERROR(_process_block(state, block));
    }

    if (eos) {
        LOG(INFO) << "EOS reached in BlackholeSink, sending cache metrics";
        // Collect final cache metrics when processing is complete
        _collect_cache_metrics(state, local_state);

        // Send cache metrics batch to FE for WARM UP SELECT results
        RETURN_IF_ERROR(_send_cache_metrics_batch(state, local_state));

        // LOG(INFO) << "BlackholeSink completed processing. "
        //            << "Rows processed: " << local_state._rows_processed
        //            << ", Bytes processed: " << local_state._bytes_processed
        //            << ", Cache read bytes: " << local_state._cache_read_bytes
        //            << ", Cache write bytes: " << local_state._cache_write_bytes;
    }

    return Status::OK();
}

Status BlackholeSinkOperatorX::_process_block(RuntimeState* state, vectorized::Block* block) {
    auto& local_state = get_local_state(state);

    LOG(INFO) << "Processing block in BlackholeSink, rows: " << block->rows()
              << ", bytes: " << block->bytes();

    // Update metrics - count rows and bytes processed
    local_state._rows_processed += block->rows();
    local_state._bytes_processed += block->bytes();

    // Update runtime counters
    if (local_state._rows_processed_timer) {
        COUNTER_UPDATE(local_state._rows_processed_timer, block->rows());
    }
    if (local_state._bytes_processed_timer) {
        COUNTER_UPDATE(local_state._bytes_processed_timer, block->bytes());
    }

    // The core BLACKHOLE behavior: discard the data
    // We don't write the block anywhere - it's effectively sent to /dev/null
    // This allows the query to execute normally (reading data, populating cache)
    // while ensuring no results are returned to the client

    LOG(INFO) << "BlackholeSink discarded block with " << block->rows() << " rows and "
              << block->bytes() << " bytes. Total processed: rows=" << local_state._rows_processed
              << ", bytes=" << local_state._bytes_processed;

    return Status::OK();
}

void BlackholeSinkOperatorX::_collect_cache_metrics(RuntimeState* state,
                                                    BlackholeSinkLocalState& local_state) {
    // Collect cache metrics from runtime state
    // These metrics are updated during query execution by scan operators
    // local_state._cache_read_bytes = state->get_datacache_read_bytes();
    // local_state._cache_write_bytes = state->get_datacache_write_bytes();

    auto io_context = state->get_query_ctx()->resource_ctx()->io_context();
    local_state._scan_rows = io_context->scan_rows();
    local_state._scan_bytes = io_context->scan_bytes();
    local_state._scan_bytes_from_local_storage = io_context->scan_bytes_from_local_storage();
    local_state._scan_bytes_from_remote_storage = io_context->scan_bytes_from_remote_storage();

    // Update performance counters for profiling
    // if (local_state._cache_read_timer) {
    //     COUNTER_UPDATE(local_state._cache_read_timer, local_state._cache_read_bytes);
    // }
    // if (local_state._cache_write_timer) {
    //     COUNTER_UPDATE(local_state._cache_write_timer, local_state._cache_write_bytes);
    // }

    VLOG_DEBUG << "Collected cache metrics for WARM UP SELECT. "
               << "Rows Processed: " << local_state._rows_processed
               << ", Bytes Processed: " << local_state._bytes_processed
               << ", Scan Rows: " << local_state._scan_rows
               << ", Scan Bytes: " << local_state._scan_bytes
               << ", Scan Bytes from Local Storage: " << local_state._scan_bytes_from_local_storage
               << ", Scan Bytes from Remote Storage: "
               << local_state._scan_bytes_from_remote_storage;
}

Status BlackholeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    // Initialize performance counters
    _rows_processed_timer = ADD_COUNTER(custom_profile(), "RowsProcessed", TUnit::UNIT);
    _bytes_processed_timer = ADD_COUNTER(custom_profile(), "BytesProcessed", TUnit::BYTES);
    _cache_read_timer = ADD_COUNTER(custom_profile(), "CacheReadBytes", TUnit::BYTES);
    // _cache_write_timer = ADD_COUNTER(custom_profile(), "CacheWriteBytes", TUnit::BYTES);

    static const std::string timer_name = "WaitForDependencyTime";
    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), timer_name, 1);
    auto fragment_instance_id = state->fragment_instance_id();

    // if (state->query_options().enable_parallel_result_sink) {
    //     _sender = _parent->cast<BlackholeSinkOperatorX>()._sender;
    //     LOG(INFO) << "Using shared sender from operator for blackhole sink, query_id: " << print_id(state->query_id())
    //               << ", finst_id: " << print_id(fragment_instance_id);
    // } else {
    // For non-parallel result sink
    VLOG_DEBUG << "create sender in INIT with instance id " << fragment_instance_id;
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
            fragment_instance_id, 4096, &_sender, state, false, nullptr));
    // }
    // For fake shared state, _dependency is expected to be null
    // We need to create a fake dependency to register the fragment instance ID
    // This is required for proper cleanup in ResultBlockBuffer::close
    if (_dependency) {
        _sender->set_dependency(fragment_instance_id, _dependency->shared_from_this());
    } else {
        // Create a fake dependency for blackhole sink
        auto fake_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                         "BlackholeSinkFakeDependency");
        _sender->set_dependency(fragment_instance_id, fake_dependency);
        LOG(INFO) << "Created fake dependency for blackhole sink, query_id: "
                  << print_id(state->query_id())
                  << ", finst_id: " << print_id(fragment_instance_id);
    }

    return Status::OK();
}

Status BlackholeSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    LOG(INFO) << "BlackholeSinkLocalState opened for WARM UP SELECT operation";
    return Status::OK();
}

Status BlackholeSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);

    // Set final cache metrics in RuntimeState for WARM UP SELECT reporting
    // state->update_datacache_read_metrics(_cache_read_bytes, 0);
    // state->update_datacache_write_metrics(_cache_write_bytes, 0);

    // LOG(INFO) << "BlackholeSinkLocalState closing. "
    //           << "Total rows processed: " << _rows_processed
    //           << ", Total bytes processed: " << _bytes_processed
    //           << ", Cache read bytes: " << _cache_read_bytes
    //           << ", Cache write bytes: " << _cache_write_bytes;

    // Close the result buffer properly (similar to ResultSinkLocalState::close)
    if (_sender) {
        auto fragment_instance_id = state->fragment_instance_id();
        int64_t processed_rows = _rows_processed;
        LOG(INFO) << "Closing result buffer for blackhole sink, query_id: "
                  << print_id(state->query_id())
                  << ", finst_id: " << print_id(fragment_instance_id);
        Status close_status = _sender->close(fragment_instance_id, exec_status, processed_rows);
        if (!close_status.ok()) {
            LOG(WARNING) << "Failed to close result buffer: " << close_status
                         << ", fragment_instance_id: " << print_id(fragment_instance_id);
        }
    }

    return Base::close(state, exec_status);
}

Status BlackholeSinkOperatorX::close(RuntimeState* state) {
    // LOG(INFO) << "BlackholeSink completed. Processed rows: " << local_state._rows_processed
    //           << ", bytes: " << local_state._bytes_processed
    //           << ", cache read: " << local_state._cache_read_bytes
    //           << ", cache write: " << local_state._cache_write_bytes;

    return Status::OK();
}

void BlackholeSinkOperatorX::get_metrics(RuntimeState* state, int64_t& rows, int64_t& bytes,
                                         int64_t& cache_read_bytes, int64_t& cache_write_bytes) {
    auto& local_state = get_local_state(state);

    // Final collection of cache metrics
    _collect_cache_metrics(state, local_state);

    // rows = local_state._rows_processed;
    // bytes = local_state._bytes_processed;
    // cache_read_bytes = local_state._cache_read_bytes;
    // cache_write_bytes = local_state._cache_write_bytes;

    // VLOG_DEBUG << "BlackholeSink metrics: "
    //            << "rows=" << rows << ", bytes=" << bytes << ", cache_read=" << cache_read_bytes
    //            << ", cache_write=" << cache_write_bytes;
}

Status BlackholeSinkOperatorX::_send_cache_metrics_batch(RuntimeState* state,
                                                         BlackholeSinkLocalState& local_state) {
    // Send cache metrics as a result batch to FE
    // This ensures FE receives WARM UP SELECT results, similar to VFileResultWriter::_send_result()

    LOG(INFO) << "Attempting to send cache metrics batch, sender is "
              << (local_state._sender ? "not null" : "null");

    if (!local_state._sender) {
        LOG(INFO) << "No result sender available, skipping cache metrics batch";
        return Status::OK();
    }

    // The cache metrics result include:
    // | RowsProcessed    | Bigint  |
    // | BytesProcessed   | Bigint  |
    // | CacheReadBytes   | Bigint  |
    // | CacheWriteBytes  | Bigint  |
    // Use MysqlRowBuffer to build MySQL protocol compliant row data
    MysqlRowBuffer<> row_buffer;

    // Push values for each column
    row_buffer.push_bigint(local_state._rows_processed);                // RowsProcessed
    row_buffer.push_bigint(local_state._bytes_processed);               // BytesProcessed
    row_buffer.push_bigint(local_state._scan_rows);                     // ScanRows
    row_buffer.push_bigint(local_state._scan_bytes);                    // ScanBytes
    row_buffer.push_bigint(local_state._scan_bytes_from_local_storage); // ScanBytesFromLocalStorage
    row_buffer.push_bigint(
            local_state._scan_bytes_from_remote_storage); // ScanBytesFromRemoteStorage

    // Create the result batch
    auto result = std::make_shared<TFetchDataResult>();
    result->result_batch.rows.resize(1);
    result->result_batch.rows[0].assign(row_buffer.buf(), row_buffer.length());

    // // Add attach_infos for additional metadata
    // std::map<std::string, std::string> attach_infos;
    // attach_infos.insert(std::make_pair("RowsProcessed", std::to_string(local_state._rows_processed)));
    // attach_infos.insert(std::make_pair("BytesProcessed", std::to_string(local_state._bytes_processed)));
    // attach_infos.insert(std::make_pair("ScanRows", std::to_string(local_state._scan_rows)));
    // attach_infos.insert(std::make_pair("ScanBytes", std::to_string(local_state._scan_bytes)));
    // attach_infos.insert(std::make_pair("ScanBytesFromLocalStorage", std::to_string(local_state._scan_bytes_from_local_storage)));
    // attach_infos.insert(std::make_pair("ScanBytesFromRemoteStorage", std::to_string(local_state._scan_bytes_from_remote_storage)));

    // result->result_batch.__set_attached_infos(attach_infos);

    // Send the batch through the result buffer (like VFileResultWriter does)
    // LOG(INFO) << "Sending cache metrics batch to FE - Rows: " << local_state._rows_processed
    //           << ", Bytes: " << local_state._bytes_processed
    //           << ", CacheRead: " << local_state._cache_read_bytes
    //           << ", CacheWrite: " << local_state._cache_write_bytes;

    // Cast to MySQLResultBlockBuffer to access add_batch method
    auto mysql_sender =
            std::dynamic_pointer_cast<vectorized::MySQLResultBlockBuffer>(local_state._sender);
    Status status;
    if (mysql_sender) {
        status = mysql_sender->add_batch(state, result);
    } else {
        status = Status::InternalError("Failed to cast sender to MySQLResultBlockBuffer");
    }

    if (!status.ok()) {
        LOG(WARNING) << "Failed to send cache metrics batch to FE: " << status;
        return status;
    }

    LOG(INFO) << "Successfully sent cache metrics batch to FE - Rows Processed: "
              << local_state._rows_processed
              << ", Bytes Processed: " << local_state._bytes_processed
              << ", ScanRows: " << local_state._scan_rows
              << ", ScanBytes: " << local_state._scan_bytes
              << ", ScanBytesFromLocalStorage: " << local_state._scan_bytes_from_local_storage
              << ", ScanBytesFromRemoteStorage: " << local_state._scan_bytes_from_remote_storage;
    return Status::OK();
}

} // namespace pipeline
} // namespace doris
