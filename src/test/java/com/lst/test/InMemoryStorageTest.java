package com.lst.test;

import com.lsmt.model.SegmentEntry;
import com.lsmt.storage.DiskStorage;
import com.lsmt.storage.InMemoryStorage;
import com.lsmt.storage.SegmentImpl;
import com.lsmt.storage.WriteAheadLog;
import org.junit.Before;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.util.UUID;

public class InMemoryStorageTest {

    InMemoryStorage storage;
    DiskStorage diskStorage;

    @Before
    public void setup() throws Exception {
        diskStorage = new DiskStorage();
        storage = new InMemoryStorage(diskStorage, new WriteAheadLog());
        storage.setSize(10);
        diskStorage.setLocation("/Users/manoj/kvdata");
        diskStorage.populateSegments();
    }

    @Test
    public void put() {
        for (int i = 0; i <= 20; i++) {
            storage.put("key-" + i, "value-" + i);
        }
        storage.put("Manoj", "TestINg");
        storage.put("Asdf", "testing");
    }

    @Test
    public void get() {
        System.out.println(storage.get("key-12"));
    }


    @Test
    public void testReadLastEntry() throws Exception {
        String fileName = "/Users/manoj/kvdata/segment-a341eb04-de7e-44a3-bd04-10ae25add6eb-offset";
        RandomAccessFile file = new RandomAccessFile(fileName, "r");
        SegmentImpl segment = new SegmentImpl("/Users/manoj/kvdata/segment-a341eb04-de7e-44a3-bd04-10ae25add6eb");
        System.out.println(segment.getLastValueLen());
        System.out.println(file.length());
    }

    @Test
    public void testAppendSegment() throws Exception {
        SegmentImpl segment = SegmentImpl.create("segment" + UUID.randomUUID());
        for (int i = 0; i < 100; i++) {
            segment.appendSegmentEntry(new SegmentEntry("key-" + i, "value-" + i));
        }
    }
}
