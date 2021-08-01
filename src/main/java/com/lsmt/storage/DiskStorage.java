package com.lsmt.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


@Slf4j
@Component("diskStorage")
public class DiskStorage implements Storage {

    private final List<Segment> segments = new ArrayList<>();

    @Value("${segments.location}")
    public String location;

    public DiskStorage() {

    }

    @PostConstruct
    public void populateSegments() {
        for (File file : Objects.requireNonNull(new File(location).listFiles((dir, name) -> !name.contains("offset") &&
                !name.contains(".DS_Store") && !name.contains("wal")))) {
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

    @Override
    public void delete(String key) {
        //NO-OP
    }

    @Override
    public void update(String key, String value) {
        //NO-OP
    }
}
