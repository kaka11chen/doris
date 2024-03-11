#pragma once

#include <algorithm>
#include <functional>
#include <iostream>
#include <vector>

#include "vec/core/block.h"
#include "vec/exec/skewed_partition_rebalancer.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::vectorized {

template <typename PartitionFunction>
class ScaleWriterPartitioningExchanger {
private:
    int _channel_size;
    long maxBufferedBytes;
    double SCALE_WRITER_MEMORY_PERCENTAGE;
    PartitionFunction& _partition_function;
    SkewedPartitionRebalancer& _partitionRebalancer;
    std::vector<int> partitionRowCounts;
    std::vector<int> partitionWriterIds;
    std::vector<int> partitionWriterIndexes;
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
              _partitionRebalancer(partitionRebalancer),
              partitionRowCounts(partitionCount, 0),
              partitionWriterIds(partitionCount, -1),
              partitionWriterIndexes(partitionCount, 0),
              totalMemoryUsed(totalMemoryUsed),
              maxMemoryPerNode(maxMemoryPerNode) {}

    std::vector<std::vector<uint32>> accept(Block* block) {
        std::vector<std::vector<uint32>> writerAssignments(_channel_size, std::vector<uint32>());
        // Reset the value of partition row count, writer ids and data processed for this page
        //        long dataProcessed = 0;
        for (int partitionId = 0; partitionId < partitionRowCounts.size(); partitionId++) {
            partitionRowCounts[partitionId] = 0;
            partitionWriterIds[partitionId] = -1;
        }

        // Scale up writers when current buffer memory utilization is more than 50% of the maximum.
        // Do not scale up if total memory used is greater than 70% of max memory per node.
        // We have to be conservative here otherwise scaling of writers will happen first
        // before we hit this limit, and then we won't be able to do anything to stop OOM error.
        //        if (getBufferedBytes() > maxBufferedBytes * 0.5 &&
        //            totalMemoryUsed < maxMemoryPerNode * SCALE_WRITER_MEMORY_PERCENTAGE) {
        _partitionRebalancer.rebalance();
        //        }

        // Assign each row to a writer by looking at partitions scaling state using partitionRebalancer
        for (int position = 0; position < block->rows(); position++) {
            // Get row partition id (or bucket id) which limits to the partitionCount. If there are more physical partitions than
            // this artificial partition limit, then it is possible that multiple physical partitions will get assigned the same
            // bucket id. Thus, multiple partitions will be scaled together since we track partition physicalWrittenBytes
            // using the artificial limit (partitionCount).
            int partitionId = _partition_function.getPartition(block, position);
            partitionRowCounts[partitionId] += 1;

            // Get writer id for this partition by looking at the scaling state
            int writerId = partitionWriterIds[partitionId];
            if (writerId == -1) {
                writerId = getNextWriterId(partitionId);
                partitionWriterIds[partitionId] = writerId;
            }
            writerAssignments[writerId].push_back(position);
        }

        //        // Only update the scaling state if the memory used is below the SCALE_WRITER_MEMORY_PERCENTAGE limit. Otherwise, if we keep updating
        //        // the scaling state and the memory used is fluctuating around the limit, then we could do massive scaling
        //        // in a single rebalancing cycle which could cause OOM error.
        //        if (totalMemoryUsed.get() < maxMemoryPerNode * SCALE_WRITER_MEMORY_PERCENTAGE) {
        for (int partitionId = 0; partitionId < partitionRowCounts.size(); partitionId++) {
            _partitionRebalancer.addPartitionRowCount(partitionId, partitionRowCounts[partitionId]);
        }
        _partitionRebalancer.addDataProcessed(block->bytes());
        //        }

        return writerAssignments;
    }

    long getBufferedBytes() {
        // Dummy implementation
        return 0;
    }

    int getNextWriterId(int partitionId) {
        return _partitionRebalancer.getTaskId(partitionId, partitionWriterIndexes[partitionId]++);
    }
};

//int main() {
//    // Dummy implementations for buffers, partitionRebalancer, totalMemoryUsed
//    vector<std::function<void(int)>> buffers;
//    std::function<void()> partitionRebalancer;
//    std::function<long()> totalMemoryUsed;
//    long maxMemoryPerNode = 1024 * 1024 * 1024; // 1GB
//    long maxBufferedBytes = 512 * 1024 * 1024;  // 512MB
//    double SCALE_WRITER_MEMORY_PERCENTAGE = 0.7;
//
//    // Create ScaleWriterPartitioningExchanger instance
//    ScaleWriterPartitioningExchanger exchanger(buffers, maxBufferedBytes,
//                                               SCALE_WRITER_MEMORY_PERCENTAGE, partitionRebalancer,
//                                               4, // Partition count
//                                               totalMemoryUsed, maxMemoryPerNode);
//
//    // Accept a page
//    exchanger.accept(100);
//
//    return 0;
//}
} // namespace doris::vectorized
