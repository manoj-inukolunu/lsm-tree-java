package com.lst.test;

import com.lst.storage.DiskStorage;
import com.lst.storage.InMemoryStorage;
import com.lst.storage.WriteAheadLog;
import org.junit.Before;
import org.junit.Test;

public class InMemoryStorageTest {

    InMemoryStorage storage;
    DiskStorage diskStorage;

    @Before
    public void setup() throws Exception {
        diskStorage = new DiskStorage();
        storage = new InMemoryStorage(diskStorage, new WriteAheadLog());
    }

    @Test
    public void put() {
        for (int i = 0; i <= 20; i++) {
            storage.put("key-" + i, "value-" + i);
        }
        storage.put("Manoj", "TestINg");
        storage.put("Asdf", "testing");
        throw new RuntimeException();
    }

    @Test
    public void get() {
        System.out.println(storage.get("key-12"));
    }
}
