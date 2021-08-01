package com.lst.test;

import org.junit.Test;

import java.io.RandomAccessFile;

public class DiskStorageTest {

    @Test
    public void test() throws Exception {
        RandomAccessFile file = new RandomAccessFile("/Users/manoj/kvstore/offsets-1", "r");
    }


}
