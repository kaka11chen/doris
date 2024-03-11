#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include <set>

namespace doris {
namespace vectorized {

template <typename T>
struct IndexedPriorityQueueEntry {
    T value;
    long priority;
    long generation;

    //    IndexedPriorityQueueEntry() = default;
    IndexedPriorityQueueEntry(T val, long prio, long gen)
            : value(val), priority(prio), generation(gen) {}
};

enum class PriorityOrdering { LOW_TO_HIGH, HIGH_TO_LOW };

template <typename T, PriorityOrdering priority_ordering>
struct CustomComparator {
    bool operator()(const IndexedPriorityQueueEntry<T>& lhs,
                    const IndexedPriorityQueueEntry<T>& rhs) const {
        if constexpr (priority_ordering == PriorityOrdering::LOW_TO_HIGH) {
            if (lhs.priority != rhs.priority) {
                return lhs.priority < rhs.priority;
            }
            return lhs.generation < rhs.generation;
        } else {
            if (lhs.priority != rhs.priority) {
                return lhs.priority > rhs.priority;
            }
            return lhs.generation < rhs.generation;
        }
    }
};

template <typename T, PriorityOrdering priority_ordering = PriorityOrdering::HIGH_TO_LOW>
class IndexedPriorityQueue {
public:
    struct Prioritized {
        T value;
        long priority;
    };

    IndexedPriorityQueue() = default;

    bool addOrUpdate(const T& element, long priority) {
        auto it = index.find(element);
        if (it != index.end()) {
            if (it->second.priority == priority) {
                return false;
            }
            queue.erase(it->second);
        }
        IndexedPriorityQueueEntry<T> entry {element, priority, generation++};
        queue.insert(entry);
        index.insert({element, entry});
        return true;
    }

    bool contains(const T& element) const { return index.find(element) != index.end(); }

    bool remove(const T& element) {
        auto it = index.find(element);
        if (it != index.end()) {
            queue.erase(it->second);
            index.erase(it);
            return true;
        }
        return false;
    }

    std::optional<T> poll() {
        if (queue.empty()) {
            return std::nullopt;
        }
        T value = queue.begin()->value;
        index.erase(value);
        queue.erase(queue.begin());
        return value;
    }

    std::optional<Prioritized> peek() const {
        if (queue.empty()) {
            return std::nullopt;
        }
        const IndexedPriorityQueueEntry<T>& entry = *queue.begin();
        return Prioritized {entry.value, entry.priority};
    }

    int size() const { return queue.size(); }

    bool isEmpty() const { return queue.empty(); }

    // 迭代器定义
    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = T;
        using difference_type = std::ptrdiff_t;
        using pointer = T*;
        using reference = T&;

        Iterator() : entryIter() {}
        explicit Iterator(
                typename std::set<IndexedPriorityQueueEntry<T>,
                                  CustomComparator<T, priority_ordering>>::const_iterator iter)
                : entryIter(iter) {}

        const T& operator*() const { return entryIter->value; }

        const T* operator->() const { return &(entryIter->value); }

        Iterator& operator++() {
            ++entryIter;
            return *this;
        }

        Iterator operator++(int) {
            Iterator temp = *this;
            ++(*this);
            return temp;
        }

        bool operator==(const Iterator& other) const { return entryIter == other.entryIter; }

        bool operator!=(const Iterator& other) const { return !(*this == other); }

    private:
        typename std::set<IndexedPriorityQueueEntry<T>,
                          CustomComparator<T, priority_ordering>>::const_iterator entryIter;
    };

    Iterator begin() const { return Iterator(queue.begin()); }

    Iterator end() const { return Iterator(queue.end()); }

private:
    std::map<T, IndexedPriorityQueueEntry<T>> index;
    std::set<IndexedPriorityQueueEntry<T>, CustomComparator<T, priority_ordering>> queue;

    long generation = 0;
};

} // namespace vectorized
} // namespace doris
