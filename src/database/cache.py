from collections import OrderedDict, defaultdict
from typing import Any, Optional
import time

# Stores cache constantly
class LRUCache:
    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def __setitem__(self, key, value):
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def clear(self):
        self.cache.clear()

# Stores cache temporarily
class TTLCache:
    def __init__(self, maxsize: int = 128, ttl: float = 60.0):
        self.maxsize = maxsize
        self.ttl = ttl
        self._store = OrderedDict()

    def _expire_old(self):
        now = time.time()
        expired_keys = [k for k, (_, t) in self._store.items() if now - t > self.ttl]
        for k in expired_keys:
            del self._store[k]

    def get(self, key: Any, default: Any = None) -> Optional[Any]:
        self._expire_old()
        item = self._store.get(key)
        if item:
            value, timestamp = item
            if time.time() - timestamp <= self.ttl:
                return value
            else:
                del self._store[key]
        return default

    def __setitem__(self, key: Any, value: Any):
        self._expire_old()
        if key in self._store:
            del self._store[key]
        elif len(self._store) >= self.maxsize:
            self._store.popitem(last=False)
        self._store[key] = (value, time.time())

    def clear(self):
        self._store.clear()

    def __contains__(self, key):
        self._expire_old()
        return key in self._store

    def __len__(self):
        self._expire_old()
        return len(self._store)

class LFUCache:
    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self.key_to_val = {}  # key -> value
        self.key_to_freq = {} # key -> freq
        self.freq_to_keys = defaultdict(OrderedDict) # freq -> OrderedDict of keys
        self.min_freq = 0

    def get(self, key):
        if key not in self.key_to_val:
            return None

        self._increase_freq(key)
        return self.key_to_val[key]

    def __setitem__(self, key, value):
        if self.capacity <= 0:
            return

        if key in self.key_to_val:
            self.key_to_val[key] = value
            self._increase_freq(key)
            return

        if len(self.key_to_val) >= self.capacity:
            self._evict()

        self.key_to_val[key] = value
        self.key_to_freq[key] = 1
        self.freq_to_keys[1][key] = None
        self.min_freq = 1

    def _increase_freq(self, key):
        freq = self.key_to_freq[key]
        del self.freq_to_keys[freq][key]
        if not self.freq_to_keys[freq]:
            del self.freq_to_keys[freq]
            if freq == self.min_freq:
                self.min_freq += 1

        self.key_to_freq[key] = freq + 1
        self.freq_to_keys[freq + 1][key] = None

    def _evict(self):
        key, _ = self.freq_to_keys[self.min_freq].popitem(last=False)
        if not self.freq_to_keys[self.min_freq]:
            del self.freq_to_keys[self.min_freq]

        del self.key_to_val[key]
        del self.key_to_freq[key]
