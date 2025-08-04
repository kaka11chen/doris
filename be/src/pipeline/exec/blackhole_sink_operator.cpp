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

#include <sstream>

#include "common/logging.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

BlackholeSinkOperatorX::BlackholeSinkOperatorX(int operator_id, const int dest_id)
        : Base(operator_id, 0, dest_id) {}

Status BlackholeSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    return Status::OK();
}

Status BlackholeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)block->rows());

    if (block && block->rows() > 0) {
        // Process the block (essentially discard it but collect metrics)
        RETURN_IF_ERROR(_process_block(state, block));
    }

    if (eos) {
        // Collect final cache metrics when processing is complete
        _collect_cache_metrics(state, local_state);
        VLOG_DEBUG << "BlackholeSink completed processing. "
                   << "Rows processed: " << local_state._rows_processed
                   << ", Bytes processed: " << local_state._bytes_processed;
    }

    return Status::OK();
}

Status BlackholeSinkOperatorX::_process_block(RuntimeState* state, vectorized::Block* block) {
    auto& local_state = get_local_state(state);

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

    VLOG_DEBUG << "BlackholeSink discarded block with " << block->rows() << " rows and "
               << block->bytes() << " bytes";

    return Status::OK();
}

void BlackholeSinkOperatorX::_collect_cache_metrics(RuntimeState* state,
                                                    BlackholeSinkLocalState& local_state) {
    // Collect cache metrics from runtime state
    // These metrics are updated during query execution by scan operators
    local_state._cache_read_bytes = state->get_datacache_read_bytes();
    local_state._cache_write_bytes = state->get_datacache_write_bytes();

    // Update performance counters for profiling
    if (local_state._cache_read_timer) {
        COUNTER_UPDATE(local_state._cache_read_timer, local_state._cache_read_bytes);
    }
    if (local_state._cache_write_timer) {
        COUNTER_UPDATE(local_state._cache_write_timer, local_state._cache_write_bytes);
    }

    VLOG_DEBUG << "Collected cache metrics for WARM UP SELECT. "
               << "Rows: " << local_state._rows_processed
               << ", Bytes: " << local_state._bytes_processed
               << ", Cache read: " << local_state._cache_read_bytes
               << ", Cache write: " << local_state._cache_write_bytes;
}

Status BlackholeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    // Initialize performance counters
    _rows_processed_timer = ADD_COUNTER(custom_profile(), "RowsProcessed", TUnit::UNIT);
    _bytes_processed_timer = ADD_COUNTER(custom_profile(), "BytesProcessed", TUnit::BYTES);
    _cache_read_timer = ADD_COUNTER(custom_profile(), "CacheReadBytes", TUnit::BYTES);
    _cache_write_timer = ADD_COUNTER(custom_profile(), "CacheWriteBytes", TUnit::BYTES);

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
    state->update_datacache_read_metrics(_cache_read_bytes, 0);
    state->update_datacache_write_metrics(_cache_write_bytes, 0);

    LOG(INFO) << "BlackholeSinkLocalState closing. "
              << "Total rows processed: " << _rows_processed
              << ", Total bytes processed: " << _bytes_processed
              << ", Cache read bytes: " << _cache_read_bytes
              << ", Cache write bytes: " << _cache_write_bytes;

    return Base::close(state, exec_status);
}

void BlackholeSinkOperatorX::get_metrics(RuntimeState* state, int64_t& rows, int64_t& bytes,
                                         int64_t& cache_read_bytes, int64_t& cache_write_bytes) {
    auto& local_state = get_local_state(state);

    // Final collection of cache metrics
    _collect_cache_metrics(state, local_state);

    rows = local_state._rows_processed;
    bytes = local_state._bytes_processed;
    cache_read_bytes = local_state._cache_read_bytes;
    cache_write_bytes = local_state._cache_write_bytes;

    VLOG_DEBUG << "BlackholeSink metrics: "
               << "rows=" << rows << ", bytes=" << bytes << ", cache_read=" << cache_read_bytes
               << ", cache_write=" << cache_write_bytes;
}

} // namespace pipeline
} // namespace doris
