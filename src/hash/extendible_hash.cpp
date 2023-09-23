#include <list>
#include <algorithm>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : bucket_size_(size), global_depth_(0) {
    buckets_.push_back(std::make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    return std::hash<K>() (key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    return global_depth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    return buckets_[bucket_id]->local_depth_;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    return buckets_.size();
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    std::lock_guard<std::mutex> lockGuard(latch_);

    int bucket_idx = GetBucketIndex(key);
    std::shared_ptr<Bucket> bucket = buckets_.at(bucket_idx);
    std::list<std::pair<K, V>>& slots = bucket->slots_;

    for (auto& slot: slots) {
        if (slot.first == key) {
            value = slot.second;
            return true;
        }
    }

    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
   std::lock_guard<std::mutex> lockGuard(latch_);

   int bucket_idx = GetBucketIndex(key);
   std::shared_ptr<Bucket> bucket = buckets_.at(bucket_idx);
   std::list<std::pair<K, V>>& slots = bucket->slots_;

   for (auto it = slots.begin(); it != slots.end(); it++) {
       if (it->first == key) {
           slots.erase(it);
           return true;
       }
   }
   return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    std::lock_guard<std::mutex> lockGuard(latch_);

    int bucket_idx = GetBucketIndex(key);
    std::shared_ptr<Bucket> bucket = buckets_.at(bucket_idx);

    for (std::pair<K, V>& slot: bucket->slots_) {
        if (slot.first == key) {
            slot.second = value;
            return;
        }
    }

    while (bucket->slots_.size() >= bucket_size_) {
        if (bucket->local_depth_ == global_depth_) {
            global_depth_++;
            size_t size = buckets_.size();
            for (size_t i = 0; i < size; ++i) {
                buckets_.push_back(buckets_.at(i));
            }
        }

        int local_depth = bucket->local_depth_ + 1;
        int mask = 1 << (local_depth - 1);
        std::list<std::pair<K, V>> old_values = bucket->slots_;

        auto first = std::make_shared<Bucket>(local_depth);
        auto second = std::make_shared<Bucket>(local_depth);
        for (auto& it: old_values) {
            if (mask & HashKey(it.first)) {
                first->slots_.push_back(it);
            } else {
                second->slots_.push_back(it);
            }
        }

        for (size_t i = HashKey(key) & (mask - 1); i < buckets_.size(); i += mask) {
            buckets_[i] = (i & mask) ? first : second;
        }

        bucket_idx = GetBucketIndex(key);
        bucket = buckets_.at(bucket_idx);
    }

    bucket->slots_.push_back(std::make_pair(key, value));
}

template <typename K, typename V>
int ExtendibleHash<K, V>::GetBucketIndex(const K &k) {
    return HashKey(k) & ((1 << global_depth_) - 1);
}


template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
