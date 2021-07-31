package com.lst.storage;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class DiskStorage implements Storage {

    List<Segment> segments = new ArrayList<>();

    public DiskStorage(String location) {
        for (File file : new File(location).listFiles((dir, name) -> !name.contains("offset") && !name.contains(".DS_Store"))) {
            segments.add(new Segment(file.getAbsolutePath()));
        }
    }

    @Override
    public void put(String key, String value) {
        //NO - OP
    }

    @Override
    public String get(String key) {
        for (Segment segment : segments) {
            log.info("Searching in segment = {}", segment);
            String data = segment.checkAndReturn(key);
            if (data != null) {
                return data;
            }
        }
        return null;
    }
}
