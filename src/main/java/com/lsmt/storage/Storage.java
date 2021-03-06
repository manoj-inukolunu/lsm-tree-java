package com.lsmt.storage;

public interface Storage {

    void put(String key, String value);

    String get(String key);

    void delete(String key);

    void update(String key, String value);

}
