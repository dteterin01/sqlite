/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <list>
#include <unordered_map>
#include <mutex>
#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

namespace cmudb {

template <typename T>
class LRUReplacer : public Replacer<T> {
public:
    // do not change public interface
    LRUReplacer();

    ~LRUReplacer();

    void Insert(const T &value);

    bool Victim(T &value);

    bool Erase(const T &value);

    size_t Size();

private:
    std::mutex _latch;

    std::list<T> _queue_fifo;
    std::unordered_map<T, typename std::list<T>::iterator> _cache;
};

} // namespace cmudb
