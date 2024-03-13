#pragma once

#include <algorithm>
#include <functional>
#include <iostream>
#include <vector>

#include "vec/core/block.h"
#include "vec/exec/skewed_partition_rebalancer.h"

namespace doris::vectorized {

template <typename PartitionFunction>
class ScaleWriterPartitioningExchanger {
private:
    int _channel_size;
    long maxBufferedBytes;
    double SCALE_WRITER_MEMORY_PERCENTAGE;
    PartitionFunction& _partition_function;
    SkewedPartitionRebalancer& _partition_rebalancer;
    std::vector<int> _partition_row_counts;
    std::vector<int> _partition_writer_ids;
    std::vector<int> _partition_writer_indexes;
    long totalMemoryUsed;
    long maxMemoryPerNode;

public:
    ScaleWriterPartitioningExchanger(int channel_size, long maxBufferedBytes,
                                     double SCALE_WRITER_MEMORY_PERCENTAGE,
                                     PartitionFunction& partition_function,
                                     SkewedPartitionRebalancer& partitionRebalancer,
                                     int partitionCount, long totalMemoryUsed,
                                     long maxMemoryPerNode)
            : _channel_size(channel_size),
              maxBufferedBytes(maxBufferedBytes),
              SCALE_WRITER_MEMORY_PERCENTAGE(SCALE_WRITER_MEMORY_PERCENTAGE),
              _partition_function(partition_function),
              _partition_rebalancer(partitionRebalancer),
              _partition_row_counts(partitionCount, 0),
              _partition_writer_ids(partitionCount, -1),
              _partition_writer_indexes(partitionCount, 0),
              totalMemoryUsed(totalMemoryUsed),
              maxMemoryPerNode(maxMemoryPerNode) {}

    std::vector<std::vector<uint32_t>> accept(Block* block) {
        std::vector<std::vector<uint32_t>> writerAssignments(_channel_size,
                                                             std::vector<uint32_t>());
        // Reset the value of partition row count, writer ids and data processed for this page
        //        long dataProcessed = 0;
        for (int partition_id = 0; partition_id < _partition_row_counts.size(); partition_id++) {
            _partition_row_counts[partition_id] = 0;
            _partition_writer_ids[partition_id] = -1;
        }

        // Scale up writers when current buffer memory utilization is more than 50% of the maximum.
        // Do not scale up if total memory used is greater than 70% of max memory per node.
        // We have to be conservative here otherwise scaling of writers will happen first
        // before we hit this limit, and then we won't be able to do anything to stop OOM error.
        //        if (get_buffered_bytes() > maxBufferedBytes * 0.5 &&
        //            totalMemoryUsed < maxMemoryPerNode * SCALE_WRITER_MEMORY_PERCENTAGE) {
        _partition_rebalancer.rebalance();
        //        }

        // Assign each row to a writer by looking at partitions scaling state using partitionRebalancer
        for (int position = 0; position < block->rows(); position++) {
            // Get row partition id (or bucket id) which limits to the _partition_count. If there are more physical partitions than
            // this artificial partition limit, then it is possible that multiple physical partitions will get assigned the same
            // bucket id. Thus, multiple partitions will be scaled together since we track partition physicalWrittenBytes
            // using the artificial limit (_partition_count).
            int partition_id = _partition_function.getPartition(block, position);
            _partition_row_counts[partition_id] += 1;

            // Get writer id for this partition by looking at the scaling state
            int writer_id = _partition_writer_ids[partition_id];
            if (writer_id == -1) {
                writer_id = get_next_writer_id(partition_id);
                _partition_writer_ids[partition_id] = writer_id;
            }
            writerAssignments[writer_id].push_back(position);
        }

        //        // Only update the scaling state if the memory used is below the SCALE_WRITER_MEMORY_PERCENTAGE limit. Otherwise, if we keep updating
        //        // the scaling state and the memory used is fluctuating around the limit, then we could do massive scaling
        //        // in a single rebalancing cycle which could cause OOM error.
        //        if (totalMemoryUsed.get() < maxMemoryPerNode * SCALE_WRITER_MEMORY_PERCENTAGE) {
        for (int partition_id = 0; partition_id < _partition_row_counts.size(); partition_id++) {
            _partition_rebalancer.add_partition_row_count(partition_id,
                                                          _partition_row_counts[partition_id]);
        }
        _partition_rebalancer.add_data_processed(block->bytes());
        //        }

        return writerAssignments;
    }

    int get_next_writer_id(int partition_id) {
        return _partition_rebalancer.get_task_id(partition_id,
                                                 _partition_writer_indexes[partition_id]++);
    }
};

} // namespace doris::vectorized