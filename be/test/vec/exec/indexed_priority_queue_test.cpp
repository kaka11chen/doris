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

#include "vec/exec/indexed_priority_queue.h"

#include <gtest/gtest.h>

namespace doris::vectorized {

class IndexedPriorityQueueTest : public testing::Test {
public:
    IndexedPriorityQueueTest() = default;
    virtual ~IndexedPriorityQueueTest() = default;
};

TEST_F(IndexedPriorityQueueTest, test_normal) {
    std::cout << "hello" << std::endl;
    // 创建一个优先队列，按照优先级从低到高排序
    IndexedPriorityQueue<int, PriorityOrdering::HIGH_TO_LOW> pq;

    // 向优先队列中添加一些元素
    pq.addOrUpdate(3, 10);
    pq.addOrUpdate(1, 5);
    pq.addOrUpdate(4, 15);
    pq.addOrUpdate(2, 8);
    pq.addOrUpdate(5, 5);
    pq.addOrUpdate(6, 5);

    // 遍历并打印优先队列中的元素
    std::cout << "Priority Queue elements: ";
    for (auto& elem : pq) {
        std::cout << elem << " ";
    }
    std::cout << std::endl;

    // 移除一个元素并打印
    int removed = 2;
    if (pq.remove(removed)) {
        std::cout << "Removed element " << removed << std::endl;
    } else {
        std::cout << "Element " << removed << " not found" << std::endl;
    }

    // 再次遍历并打印优先队列中的元素
    std::cout << "Priority Queue elements after removal: ";
    for (auto& elem : pq) {
        std::cout << elem << " ";
    }
    std::cout << std::endl;

    // update elements
    pq.addOrUpdate(4, 1);

    // 再次遍历并打印优先队列中的元素
    std::cout << "Priority Queue elements after updating element priority: ";
    for (auto& elem : pq) {
        std::cout << elem << " ";
    }
    std::cout << std::endl;
}

} // namespace doris::vectorized
