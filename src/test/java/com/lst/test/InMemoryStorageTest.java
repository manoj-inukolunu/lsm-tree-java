package com.lst.test;

import com.lst.storage.DiskStorage;
import com.lst.storage.InMemoryStorage;
import org.junit.Before;
import org.junit.Test;

public class InMemoryStorageTest {

    InMemoryStorage storage;
    DiskStorage diskStorage;

    @Before
    public void setup() throws Exception {
        diskStorage = new DiskStorage("/Users/manoj/kvdata");
        storage = new InMemoryStorage(10, diskStorage);
    }

    @Test
    public void put() {
        for (int i = 0; i <= 20; i++) {
            storage.put("key-" + i, "value-" + i);
        }
    }

    @Test
    public void get() {
        System.out.println(storage.get("key-12"));
    }
}
