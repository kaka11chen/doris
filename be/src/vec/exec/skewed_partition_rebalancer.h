#include <climits>
#include <cmath>
#include <iostream>
#include <limits>
#include <list>
#include <queue>
#include <unordered_map>
#include <vector>

#include "vec/exec/indexed_priority_queue.h"

namespace doris::vectorized {

class SkewedPartitionRebalancer {
private:
    static constexpr int SCALE_WRITERS_PARTITION_COUNT = 4096;
    static constexpr double TASK_BUCKET_SKEWNESS_THRESHOLD = 0.7;

    int partitionCount;
    int taskCount;
    int taskBucketCount;
    long minPartitionDataProcessedRebalanceThreshold;
    long minDataProcessedRebalanceThreshold;
    std::vector<long> partitionRowCount;
    long data_processed;
    long dataProcessedAtLastRebalance;
    std::vector<long> partitionDataSize;
    std::vector<long> partitionDataSizeAtLastRebalance;
    std::vector<long> partitionDataSizeSinceLastRebalancePerTask;
    std::vector<long> estimatedTaskBucketDataSizeSinceLastRebalance;

    struct TaskBucket {
        int taskId;
        int id;

        TaskBucket(int taskId, int bucketId, int taskBucketCount)
                : taskId(taskId), id(taskId * taskBucketCount + bucketId) {}

        bool operator==(const TaskBucket& other) const { return id == other.id; }

        bool operator<(const TaskBucket& other) const { return id < other.id; }

        bool operator>(const TaskBucket& other) const { return id > other.id; }
    };

    struct TaskBucketHash {
        std::size_t operator()(const TaskBucket& bucket) const {
            // Combine hash values of taskId and id
            std::size_t hashTaskId = std::hash<int>()(bucket.taskId);
            std::size_t hashId = std::hash<int>()(bucket.id);

            // Combine the hash values
            // This is a simple way to combine two hash values into one
            return hashTaskId ^ (hashId + 0x9e3779b9 + (hashTaskId << 6) + (hashTaskId >> 2));
        }
    };

    // 定义一个自定义的比较器，用于比较两个任务的优先级
    struct TaskComparator {
        TaskComparator(std::vector<long>& partitionDataSizeSinceLastRebalancePerTask)
                : _partitionDataSizeSinceLastRebalancePerTask(
                          partitionDataSizeSinceLastRebalancePerTask) {}
        bool operator()(const int& partition1, const int& partition2) const {
            return _partitionDataSizeSinceLastRebalancePerTask[partition1] <
                   _partitionDataSizeSinceLastRebalancePerTask[partition2];
        }

    private:
        std::vector<long>& _partitionDataSizeSinceLastRebalancePerTask;
    };

    // 定义一个带有优先级的元素结构体
    template <typename T>
    struct PriorityElement {
        T value;
        int priority;

        PriorityElement(const T& value, int priority) : value(value), priority(priority) {}

        // 自定义比较函数，优先级相等时按照插入顺序排序
        bool operator<(const PriorityElement& other) const {
            return priority <= other.priority; // 如果优先级相等，返回插入顺序的优先级
        }
    };

    // 定义一个泛型的比较器，根据优先级进行比较
    template <typename T>
    struct LessPriorityComparator {
        bool operator()(const PriorityElement<T>& lhs, const PriorityElement<T>& rhs) const {
            return lhs.priority <= rhs.priority; // 优先级小的元素优先级高
        }
    };

    template <typename T>
    struct GreaterPriorityComparator {
        bool operator()(const PriorityElement<T>& lhs, const PriorityElement<T>& rhs) const {
            return lhs.priority >= rhs.priority; // 优先级小的元素优先级高
        }
    };

    std::vector<std::vector<TaskBucket>> partitionAssignments;

public:
    SkewedPartitionRebalancer(int partitionCount, int taskCount, int taskBucketCount,
                              long minPartitionDataProcessedRebalanceThreshold,
                              long maxDataProcessedRebalanceThreshold)
            : partitionCount(partitionCount),
              taskCount(taskCount),
              taskBucketCount(taskBucketCount),
              minPartitionDataProcessedRebalanceThreshold(
                      minPartitionDataProcessedRebalanceThreshold),
              minDataProcessedRebalanceThreshold(
                      std::max(minPartitionDataProcessedRebalanceThreshold,
                               maxDataProcessedRebalanceThreshold)),
              partitionRowCount(partitionCount, 0),
              data_processed(0),
              dataProcessedAtLastRebalance(0),
              partitionDataSize(partitionCount, 0),
              partitionDataSizeAtLastRebalance(partitionCount, 0),
              partitionDataSizeSinceLastRebalancePerTask(partitionCount, 0),
              estimatedTaskBucketDataSizeSinceLastRebalance(taskCount * taskBucketCount, 0),
              partitionAssignments(partitionCount) {
        //        int[] taskBucketIds = new int[taskCount];
        //        ImmutableList.Builder<List<TaskBucket>> partitionAssignments = ImmutableList.builder();
        //        for (int partition = 0; partition < partitionCount; partition++) {
        //            int taskId = partition % taskCount;
        //            int bucketId = taskBucketIds[taskId]++ % taskBucketCount;
        //            partitionAssignments.add(new CopyOnWriteArrayList<>(ImmutableList.of(new TaskBucket(taskId, bucketId))));
        //        }
        //        this.partitionAssignments = partitionAssignments.build();

        std::vector<int> taskBucketIds(taskCount, 0);

        // 构建 partitionAssignments
        for (int partition = 0; partition < partitionCount; partition++) {
            int taskId = partition % taskCount;
            int bucketId = taskBucketIds[taskId]++ % taskBucketCount;
            TaskBucket taskBucket(taskId, bucketId, taskBucketCount);
            partitionAssignments[partition].push_back({taskBucket});
        }
    }

    // 获取分区分配列表
    std::vector<std::list<int>> getPartitionAssignments() {
        std::vector<std::list<int>> assignedTasks;

        // 遍历每个分区的 TaskBucket 列表，并将 taskId 收集到 assignedTasks 中
        for (const auto& partitionAssignment : partitionAssignments) {
            std::list<int> tasks;
            // 使用 transform 将 TaskBucket 列表转换为 taskId 列表
            std::transform(partitionAssignment.begin(), partitionAssignment.end(),
                           std::back_inserter(tasks),
                           [](const TaskBucket& taskBucket) { return taskBucket.taskId; });
            assignedTasks.push_back(tasks);
        }

        return assignedTasks;
    }

    int getTaskCount() { return taskCount; }

    int getTaskId(int partitionId, int64_t index) {
        // 获取分区的 TaskBucket 列表
        const std::vector<TaskBucket>& taskIds = partitionAssignments[partitionId];

        // 计算对应的 TaskBucket 索引
        int taskIdIndex = (index % taskIds.size() + taskIds.size()) % taskIds.size();

        // 返回对应 TaskBucket 的 taskId
        return taskIds[taskIdIndex].taskId;
    }

    void addDataProcessed(long dataSize) { data_processed += dataSize; }

    void addPartitionRowCount(int partition, long rowCount) {
        partitionRowCount[partition] += rowCount;
    }

    void rebalance() {
        long currentDataProcessed = data_processed;
        if (shouldRebalance(currentDataProcessed)) {
            rebalancePartitions(currentDataProcessed);
        }
    }

    bool shouldRebalance(long dataProcessed) {
        // Rebalance only when total bytes processed since last rebalance is greater than rebalance threshold.
        return (dataProcessed - dataProcessedAtLastRebalance) >= minDataProcessedRebalanceThreshold;
    }

    void rebalancePartitions(long dataProcessed) {
        if (!shouldRebalance(dataProcessed)) {
            return;
        }

        calculatePartitionDataSize(dataProcessed);

        // Initialize partitionDataSizeSinceLastRebalancePerTask
        for (int partition = 0; partition < partitionCount; partition++) {
            int totalAssignedTasks = partitionAssignments[partition].size();
            long dataSize = partitionDataSize[partition];
            partitionDataSizeSinceLastRebalancePerTask[partition] =
                    (dataSize - partitionDataSizeAtLastRebalance[partition]) / totalAssignedTasks;
            partitionDataSizeAtLastRebalance[partition] = dataSize;
        }

        for (int partition = 0; partition < partitionCount; partition++) {
            fprintf(stderr, "partitionDataSizeSinceLastRebalancePerTask[%d]: %ld\n", partition,
                    partitionDataSizeSinceLastRebalancePerTask[partition]);
            fprintf(stderr, "partitionDataSizeAtLastRebalance[%d]: %ld\n", partition,
                    partitionDataSizeAtLastRebalance[partition]);
        }

        // Initialize taskBucketMaxPartitions
        // 初始化空的 vector
        std::vector<IndexedPriorityQueue<int, PriorityOrdering::HIGH_TO_LOW>>
                taskBucketMaxPartitions;

        // 向 vector 中添加元素
        for (int i = 0; i < taskCount * taskBucketCount; ++i) {
            taskBucketMaxPartitions.push_back(
                    IndexedPriorityQueue<int, PriorityOrdering::HIGH_TO_LOW>());
        }

        for (int partition = 0; partition < partitionCount; partition++) {
            auto& taskAssignments = partitionAssignments[partition];
            for (const auto& taskBucket : taskAssignments) {
                auto& queue = taskBucketMaxPartitions[taskBucket.id];
                queue.addOrUpdate(partition, partitionDataSizeSinceLastRebalancePerTask[partition]);
            }
        }

        // 打印每个 taskBucketMaxPartitions 中的元素
        for (auto& priorityQueue : taskBucketMaxPartitions) {
            std::cout << "taskBucketMaxPartitions Queue elements: ";
            for (auto& elem : priorityQueue) {
                std::cout << elem << " ";
            }
            std::cout << std::endl;
        }

        fprintf(stderr, "Initialize maxTaskBuckets and minTaskBuckets\n");
        // Initialize maxTaskBuckets and minTaskBuckets
        IndexedPriorityQueue<TaskBucket, PriorityOrdering::HIGH_TO_LOW> maxTaskBuckets;
        IndexedPriorityQueue<TaskBucket, PriorityOrdering::LOW_TO_HIGH> minTaskBuckets;

        for (int taskId = 0; taskId < taskCount; taskId++) {
            for (int bucketId = 0; bucketId < taskBucketCount; bucketId++) {
                TaskBucket taskBucket1(taskId, bucketId, taskBucketCount);
                TaskBucket taskBucket2(taskId, bucketId, taskBucketCount);
                estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket1.id] =
                        calculateTaskBucketDataSizeSinceLastRebalance(
                                taskBucketMaxPartitions[taskBucket1.id]);
                maxTaskBuckets.addOrUpdate(
                        std::move(taskBucket1),
                        estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket1.id]);
                minTaskBuckets.addOrUpdate(
                        std::move(taskBucket2),
                        estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket2.id]);
                //                maxTaskBuckets.addOrUpdate(taskBucket, estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
                //                minTaskBuckets.addOrUpdate(taskBucket, Long.MAX_VALUE - estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
            }
        }

        std::cout << "maxTaskBuckets: " << std::endl;
        for (auto& elem : maxTaskBuckets) {
            std::cout << "taskId: " << elem.taskId << ", id: " << elem.id << std::endl;
        }

        std::cout << "minTaskBuckets: " << std::endl;
        for (auto& elem : minTaskBuckets) {
            std::cout << "taskId: " << elem.taskId << ", id: " << elem.id << std::endl;
        }

        fprintf(stderr, "rebalanceBasedOnTaskBucketSkewness\n");
        rebalanceBasedOnTaskBucketSkewness(maxTaskBuckets, minTaskBuckets, taskBucketMaxPartitions);
        dataProcessedAtLastRebalance = dataProcessed;
    }

private:
    void calculatePartitionDataSize(long dataProcessed) {
        long totalPartitionRowCount = 0;
        for (int partition = 0; partition < partitionCount; partition++) {
            totalPartitionRowCount += partitionRowCount[partition];
        }

        for (int partition = 0; partition < partitionCount; partition++) {
            // Since we estimate the partitionDataSize based on partitionRowCount and total data processed. It is possible
            // that the estimated partitionDataSize is slightly less than it was estimated at the last rebalance cycle.
            // That's because for a given partition, row count hasn't increased, however overall data processed
            // has increased. Therefore, we need to make sure that the estimated partitionDataSize should be
            // at least partitionDataSizeAtLastRebalance. Otherwise, it will affect the ordering of minTaskBuckets
            // priority queue.
            partitionDataSize[partition] = std::max(
                    (partitionRowCount[partition] * dataProcessed) / totalPartitionRowCount,
                    partitionDataSize[partition]);
        }
        for (int partition = 0; partition < partitionCount; partition++) {
            fprintf(stderr, "partitionDataSize[%d]: %ld\n", partition,
                    partitionDataSize[partition]);
        }
    }

    long calculateTaskBucketDataSizeSinceLastRebalance(
            IndexedPriorityQueue<int, PriorityOrdering::HIGH_TO_LOW>& maxPartitions) {
        long estimatedDataSizeSinceLastRebalance = 0;
        for (auto& elem : maxPartitions) {
            estimatedDataSizeSinceLastRebalance += partitionDataSizeSinceLastRebalancePerTask[elem];
        }
        return estimatedDataSizeSinceLastRebalance;
    }

    void rebalanceBasedOnTaskBucketSkewness(
            IndexedPriorityQueue<TaskBucket, PriorityOrdering::HIGH_TO_LOW>& maxTaskBuckets,
            IndexedPriorityQueue<TaskBucket, PriorityOrdering::LOW_TO_HIGH>& minTaskBuckets,
            std::vector<IndexedPriorityQueue<int, PriorityOrdering::HIGH_TO_LOW>>&
                    taskBucketMaxPartitions) {
        std::vector<int> scaledPartitions;
        while (true) {
            std::optional<TaskBucket> maxTaskBucket = maxTaskBuckets.poll();
            if (!maxTaskBucket.has_value()) {
                break;
            }

            IndexedPriorityQueue<int, PriorityOrdering::HIGH_TO_LOW>& maxPartitions =
                    taskBucketMaxPartitions[maxTaskBucket->id];
            if (maxPartitions.isEmpty()) {
                continue;
            }

            std::vector<TaskBucket> minSkewedTaskBuckets =
                    findSkewedMinTaskBuckets(maxTaskBucket.value(), minTaskBuckets);
            if (minSkewedTaskBuckets.empty()) {
                break;
            }

            while (true) {
                std::optional<int> maxPartition = maxPartitions.poll();
                if (!maxPartition.has_value()) {
                    break;
                }
                int maxPartitionValue = maxPartition.value();

                // Rebalance partition only once in a single cycle. Otherwise, rebalancing will happen quite
                // aggressively in the early stage of write, while it is not required. Thus, it can have an impact on
                // output file sizes and resource usage such that produced files can be small and memory usage
                // might be higher.
                if (std::find(scaledPartitions.begin(), scaledPartitions.end(),
                              maxPartitionValue) != scaledPartitions.end()) {
                    continue;
                }

                int totalAssignedTasks = partitionAssignments[maxPartitionValue].size();
                if (partitionDataSize[maxPartitionValue] >=
                    (minPartitionDataProcessedRebalanceThreshold * totalAssignedTasks)) {
                    for (const TaskBucket& minTaskBucket : minSkewedTaskBuckets) {
                        if (rebalancePartition(maxPartitionValue, minTaskBucket, maxTaskBuckets,
                                               minTaskBuckets)) {
                            scaledPartitions.push_back(maxPartitionValue);
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }

    std::vector<TaskBucket> findSkewedMinTaskBuckets(
            const TaskBucket& maxTaskBucket,
            const IndexedPriorityQueue<TaskBucket, PriorityOrdering::LOW_TO_HIGH>& minTaskBuckets) {
        std::vector<TaskBucket> minSkewedTaskBuckets;

        //        while (!minTaskBucketsCopy.empty()) {
        //            TaskBucket minTaskBucket = minTaskBucketsCopy.top().value; // 将队列的顶部元素添加到容器中
        //            double skewness =
        //                    static_cast<double>(
        //                            estimatedTaskBucketDataSizeSinceLastRebalance[maxTaskBucket.id] -
        //                            estimatedTaskBucketDataSizeSinceLastRebalance[minTaskBucket.id]) /
        //                    estimatedTaskBucketDataSizeSinceLastRebalance[maxTaskBucket.id];
        //            if (skewness <= TASK_BUCKET_SKEWNESS_THRESHOLD || std::isnan(skewness)) {
        //                break;
        //            }
        //            if (maxTaskBucket.taskId != minTaskBucket.taskId) {
        //                minSkewedTaskBuckets.push_back(minTaskBucket);
        //            }
        //            minTaskBucketsCopy.pop(); // 弹出队列的顶部元素
        //        }
        for (const auto& minTaskBucket : minTaskBuckets) {
            double skewness =
                    static_cast<double>(
                            estimatedTaskBucketDataSizeSinceLastRebalance[maxTaskBucket.id] -
                            estimatedTaskBucketDataSizeSinceLastRebalance[minTaskBucket.id]) /
                    estimatedTaskBucketDataSizeSinceLastRebalance[maxTaskBucket.id];
            if (skewness <= TASK_BUCKET_SKEWNESS_THRESHOLD || std::isnan(skewness)) {
                break;
            }
            if (maxTaskBucket.taskId != minTaskBucket.taskId) {
                minSkewedTaskBuckets.push_back(minTaskBucket);
            }
        }
        return minSkewedTaskBuckets;
    }

    bool rebalancePartition(
            int partitionId, const TaskBucket& toTaskBucket,
            IndexedPriorityQueue<TaskBucket, PriorityOrdering::HIGH_TO_LOW>& maxTaskBuckets,
            IndexedPriorityQueue<TaskBucket, PriorityOrdering::LOW_TO_HIGH>& minTaskBuckets) {
        std::vector<TaskBucket>& assignments = partitionAssignments[partitionId];
        if (std::any_of(assignments.begin(), assignments.end(),
                        [&toTaskBucket](const TaskBucket& taskBucket) {
                            return taskBucket.taskId == toTaskBucket.taskId;
                        })) {
            return false;
        }

        assignments.push_back(toTaskBucket);

        int newTaskCount = assignments.size();
        int oldTaskCount = newTaskCount - 1;
        // Since a partition is rebalanced from max to min skewed taskBucket, decrease the priority of max
        // taskBucket as well as increase the priority of min taskBucket.
        for (const TaskBucket& taskBucket : assignments) {
            if (taskBucket == toTaskBucket) {
                estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id] +=
                        (partitionDataSizeSinceLastRebalancePerTask[partitionId] * oldTaskCount) /
                        newTaskCount;
            } else {
                estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id] -=
                        partitionDataSizeSinceLastRebalancePerTask[partitionId] / newTaskCount;
            }
            //            maxTasks.addOrUpdate(taskBucket, estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
            //            minTasks.addOrUpdate(taskBucket, Long.MAX_VALUE - estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
            maxTaskBuckets.addOrUpdate(
                    taskBucket, estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
            minTaskBuckets.addOrUpdate(
                    taskBucket, estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
        }

        //        log.debug("Rebalanced partition %d to task %d with taskCount %zu", partitionId, toTaskBucket.taskId, assignments.size());
        fprintf(stderr, "Rebalanced partition %d to task %d with taskCount %ld\n", partitionId,
                toTaskBucket.taskId, assignments.size());
        return true;
    }
};
} // namespace doris::vectorized
