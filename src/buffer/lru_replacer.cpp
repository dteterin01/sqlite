/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T>
void LRUReplacer<T>::Insert(const T &value) {
    std::lock_guard<std::mutex> lockGuard(_latch);

    auto pos = _cache.find(value);
    if (pos != _cache.end()) {
        _queue_fifo.erase(pos->second);
    }

    _queue_fifo.push_front(value);
    _cache.emplace(value, _queue_fifo.begin());
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T>
bool LRUReplacer<T>::Victim(T &value) {
    std::lock_guard<std::mutex> lockGuard(_latch);

    if (_cache.empty()) {
        return false;
    }

    value = _queue_fifo.back();
    _queue_fifo.pop_back();
    _cache.erase(value);

    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T>
bool LRUReplacer<T>::Erase(const T &value) {
    std::lock_guard<std::mutex> lockGuard(_latch);

    auto pos = _cache.find(value);
    if (pos == _cache.end()) {
        return false;
    }

    _queue_fifo.erase(pos->second);
    _cache.erase(pos);
    return true;
}

template <typename T> size_t LRUReplacer<T>::Size() {
    return _cache.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
