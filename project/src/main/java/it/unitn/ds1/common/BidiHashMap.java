package it.unitn.ds1.common;

import java.util.HashMap;
import java.util.Map;

public class BidiHashMap<T, U> extends HashMap<T, U> {

    private final HashMap<U, T> reverseHashMap;

    public BidiHashMap() {
        this.reverseHashMap = new HashMap<>();
    }

    @Override
    public U put(T key, U value) {
        this.reverseHashMap.put(value, key);
        return super.put(key, value);
    }

    @Override
    public U putIfAbsent(T key, U value) {
       if (this.get(key) == null) {
           return put(key, value);
       } else {
           return get(key);
       }
    }

    public T getKey(U value) {
        return reverseHashMap.get(value);
    }

    @Override
    public void putAll(Map<? extends T, ? extends U> m) {
        for (Map.Entry<? extends T, ? extends U> e : m.entrySet()) {
            T key = e.getKey();
            U value = e.getValue();
            put(key, value);
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        return super.remove(key, value) && this.reverseHashMap.remove(value, key);
    }
}
