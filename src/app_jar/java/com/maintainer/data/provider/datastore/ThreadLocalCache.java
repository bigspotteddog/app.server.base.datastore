package com.maintainer.data.provider.datastore;

import java.util.HashMap;
import java.util.Map;

import com.maintainer.data.provider.Key;

public class ThreadLocalCache {
    private static final ThreadLocal<ThreadLocalCache> cache = new ThreadLocal<ThreadLocalCache>() {
        @Override
        protected ThreadLocalCache initialValue() {
            return new ThreadLocalCache();
        }
    };

    private Map<Key, Object> map = new HashMap<Key, Object>();

    private Map<Key, Object> getMap() {
        return map;
    }

    public Object get(final Key key) {
        ThreadLocalCache c = getThreadCache();
        return c.getMap().get(key);
    }

    public void put(final Key key, final Object obj) {
        ThreadLocalCache c = getThreadCache();
        c.getMap().remove(key);
        c.getMap().put(key, obj);
    }

    public Object remove(final Key key) {
        ThreadLocalCache c = getThreadCache();
        return c.getMap().remove(key);
    }

    public static ThreadLocalCache get() {
        return getThreadCache();
    }

    private static ThreadLocalCache getThreadCache() {
        ThreadLocalCache callerCache = cache.get();
        if (callerCache == null) {
            callerCache = new ThreadLocalCache();
            cache.set(callerCache);
        }
        return callerCache;
    }

    public static void clearCache() {
        cache.set(null);
    }
}
