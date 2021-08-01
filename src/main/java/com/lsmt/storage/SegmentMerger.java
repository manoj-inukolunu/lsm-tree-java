package com.lsmt.storage;


import com.lsmt.model.SegmentEntry;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class SegmentMerger {


    @Data
    @AllArgsConstructor
    class Pair {
        int idx;
        SegmentEntry entry;
    }

    @Value("${segments.location}")
    public String location;

    @Scheduled(fixedDelay = 10000)
    public void merge() throws IOException {
        List<Segment> segments = new ArrayList<>();
        List<SegmentIterator> segmentFiles = Arrays.stream(Objects.requireNonNull(new File(location)
                .listFiles((dir, name) -> name.contains("segment") && !name.contains("merged"))))
                .map(file -> {
                    try {
                        segments.add(new SegmentImpl(file.getName()));
                        return new SegmentImpl(file.getName()).getIterator();
                    } catch (IOException e) {
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toList());
        Segment merged = SegmentImpl.create(location + "/segment-merged." + UUID.randomUUID());
        PriorityQueue<Pair> priorityQueue = new PriorityQueue<>();
        for (int i = 0; i < segmentFiles.size(); i++) {
            if (segmentFiles.get(i).hasNext()) {
                priorityQueue.add(new Pair(i, segmentFiles.get(i).next()));
            }
        }
        while (!priorityQueue.isEmpty()) {
            Pair next = priorityQueue.poll();
            merged.appendSegmentEntry(next.getEntry());
            if (segmentFiles.get(next.getIdx()).hasNext()) {
                priorityQueue.add(new Pair(next.idx, segmentFiles.get(next.getIdx()).next()));
            }
        }
        for (Segment segment : segments) {
            segment.delete();
        }

    }
}
