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

#include "vec/sink/scale_writer_partitioning_exchanger.h"

#include <gtest/gtest.h>
#include <vec/data_types/data_type_number.h>

#include <list>

namespace doris::vectorized {

class ScaleWriterPartitioningExchangerTest : public testing::Test {
public:
    ScaleWriterPartitioningExchangerTest() = default;
    virtual ~ScaleWriterPartitioningExchangerTest() = default;
};

//std::vector<std::list<int>> get_partition_positions(
//        std::unique_ptr<SkewedPartitionRebalancer>& rebalancer,
//        std::vector<long>& partitionRowCount, int partition_count, int max_position) {
//    std::vector<std::list<int>> partitionPositions(rebalancer->getTaskCount());
//
//    // 初始化每个分区的位置列表
//    for (int partition = 0; partition < rebalancer->getTaskCount(); partition++) {
//        partitionPositions[partition] = std::list<int>();
//    }
//
//    // 遍历每个位置，确定其所属的分区，并将位置添加到对应的分区位置列表中
//    for (int position = 0; position < max_position; position++) {
//        int partition = position % partition_count;
//        partition = rebalancer->getTaskId(partition, partitionRowCount[partition]++);
//        fprintf(stderr, "partition: %d, position: %d\n", partition, position);
//        partitionPositions[partition].push_back(position);
//    }
//
//    return partitionPositions;
//}
//
//// 检查两个列表是否相等
//bool listsEqual(const std::list<int>& list1, const std::list<int>& list2) {
//    return list1 == list2;
//}
//
//// 检查两个向量是否相等
//bool vectorsEqual(const std::vector<std::list<int>>& vec1,
//                  const std::vector<std::list<int>>& vec2) {
//    if (vec1.size() != vec2.size()) {
//        return false;
//    }
//    for (size_t i = 0; i < vec1.size(); i++) {
//        if (!listsEqual(vec1[i], vec2[i])) {
//            return false;
//        }
//    }
//    return true;
//}
//
//bool compareVectorOfLists(const std::vector<std::list<int>>& expected,
//                          const std::vector<std::list<int>>& actual) {
//    if (expected.size() != actual.size()) {
//        return false;
//    }
//
//    for (size_t i = 0; i < expected.size(); ++i) {
//        if (expected[i] != actual[i]) {
//            return false;
//        }
//    }
//
//    return true;
//}

class TestPartitionFunction {
public:
    TestPartitionFunction(int partition_count) : _partition_count(partition_count) {}
    int partitionCount() { return _partition_count; }

    int getPartition(Block* block, int position) { return position % _partition_count; }

private:
    std::vector<int> _partition_indexes;
    int _partition_count;
};

class SkewedPartitionFunction {
public:
    SkewedPartitionFunction(int partition_count) : _partition_count(partition_count) {}
    int partitionCount() { return _partition_count; }

    int getPartition(Block* block, int position) { return _partition_count - 1; }

private:
    std::vector<int> _partition_indexes;
    int _partition_count;
};

TEST_F(ScaleWriterPartitioningExchangerTest, test_normal) {
    const int partitionCount = 100;
    const int taskCount = 3 * 8;
    const int taskBucketCount = 1;
    const long MEGABYTE = 1024 * 1024;
    const long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = 1 * MEGABYTE; // 1MB
    const long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = 50 * MEGABYTE;          // 50MB

    long totalMemoryUsed = 0L;
    long maxMemoryPerNode = 1024 * 1024 * 1024; // 1GB
    long maxBufferedBytes = 512 * 1024 * 1024;  // 512MB
    double SCALE_WRITER_MEMORY_PERCENTAGE = 0.7;

    std::unique_ptr<SkewedPartitionRebalancer> rebalancer(
            new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount,
                                          MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD,
                                          MIN_DATA_PROCESSED_REBALANCE_THRESHOLD));
    TestPartitionFunction partition_function(partitionCount);
    std::unique_ptr<ScaleWriterPartitioningExchanger<TestPartitionFunction>>
            scale_writer_partitioning_exchanger(
                    new ScaleWriterPartitioningExchanger<TestPartitionFunction>(
                            taskCount, maxBufferedBytes, SCALE_WRITER_MEMORY_PERCENTAGE,
                            partition_function, *rebalancer, partitionCount, totalMemoryUsed,
                            maxMemoryPerNode));

    int total_rows = 4096;
    auto col1 = vectorized::ColumnVector<int>::create();
    auto& data1 = col1->get_data();
    for (int i = 0; i < total_rows; ++i) {
        data1.push_back(i);
    }

    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName type_and_name(col1->get_ptr(), data_type, "test_int");
    vectorized::Block block({type_and_name});

    std::vector<std::vector<uint32>> assignments =
            scale_writer_partitioning_exchanger->accept(&block);
    for (auto& assignment : assignments) {
        fprintf(stderr, "assignment{");
        for (auto& position : assignment) {
            fprintf(stderr, "%d ", position);
        }
        fprintf(stderr, "}\n");
    }
}

TEST_F(ScaleWriterPartitioningExchangerTest, test_skewed) {
    const int partitionCount = 100;
    const int taskCount = 3 * 8;
    const int taskBucketCount = 1;
    //    const long MEGABYTE = 1024 * 1024;
    const long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = 1024; // 120MB
    const long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = 1024;           // 200MB

    long totalMemoryUsed = 0L;
    long maxMemoryPerNode = 1024 * 1024 * 1024; // 1GB
    long maxBufferedBytes = 512 * 1024 * 1024;  // 512MB
    double SCALE_WRITER_MEMORY_PERCENTAGE = 0.7;

    std::unique_ptr<SkewedPartitionRebalancer> rebalancer(
            new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount,
                                          MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD,
                                          MIN_DATA_PROCESSED_REBALANCE_THRESHOLD));
    SkewedPartitionFunction partition_function(partitionCount);
    std::unique_ptr<ScaleWriterPartitioningExchanger<SkewedPartitionFunction>>
            scale_writer_partitioning_exchanger(
                    new ScaleWriterPartitioningExchanger<SkewedPartitionFunction>(
                            taskCount, maxBufferedBytes, SCALE_WRITER_MEMORY_PERCENTAGE,
                            partition_function, *rebalancer, partitionCount, totalMemoryUsed,
                            maxMemoryPerNode));

    int total_rows = 4096;
    auto col1 = vectorized::ColumnVector<int>::create();
    auto& data1 = col1->get_data();
    for (int i = 0; i < total_rows; ++i) {
        data1.push_back(i);
    }

    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName type_and_name(col1->get_ptr(), data_type, "test_int");
    vectorized::Block block({type_and_name});

    {
        std::vector<std::vector<uint32>> assignments =
                scale_writer_partitioning_exchanger->accept(&block);
        fprintf(stderr, "assignments.size(): %ld\n", assignments.size());
        for (auto& assignment : assignments) {
            fprintf(stderr, "assignment{");
            for (auto& position : assignment) {
                fprintf(stderr, "%d ", position);
            }
            fprintf(stderr, "}\n");
        }
    }

    {
        std::vector<std::vector<uint32>> assignments =
                scale_writer_partitioning_exchanger->accept(&block);
        for (auto& assignment : assignments) {
            fprintf(stderr, "assignment{");
            for (auto& position : assignment) {
                fprintf(stderr, "%d ", position);
            }
            fprintf(stderr, "}\n");
        }
    }
}

} // namespace doris::vectorized
