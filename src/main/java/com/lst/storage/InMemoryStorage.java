package com.lst.storage;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class InMemoryStorage implements Storage {

    int size;

    TreeMap<String, String> map = new TreeMap<>();
    ExecutorService service = Executors.newCachedThreadPool();
    int segmentCount = 0;

    Storage diskStorage;

    public InMemoryStorage(int size, Storage diskStorage) {
        this.size = size;
        this.diskStorage = diskStorage;
    }


    @Override
    public void put(String key, String value) {
        if (map.size() >= size) {
            flushToDisk(map);
            map = new TreeMap<>();
        }
        map.put(key, value);
    }


    private void flushToDisk(TreeMap<String, String> map) {
        TreeMap<String, String> copyMap = new TreeMap<>(map);
        Future future = service.submit(() -> {
            Iterator<Map.Entry<String, String>> iterator = copyMap.entrySet().iterator();
            try {
                String segmentFileName = getSegmentFileName("/Users/manoj/kvdata");
                FileWriter writer = new FileWriter(segmentFileName);
                FileWriter offsetFileWriter = new FileWriter(segmentFileName + "-offset");
                int currentOffset = 0;
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    writer.append(entry.getKey()).append(entry.getValue());
                    writer.flush();
                    offsetFileWriter.append(String.valueOf(currentOffset));
                    currentOffset += entry.getKey().length();
                    offsetFileWriter.append(" ").append(String.valueOf(currentOffset)).append("\r\n");
                    offsetFileWriter.flush();
                    currentOffset += entry.getValue().length();
                }
                writer.close();
                offsetFileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private String getSegmentFileName(String dir) {
        if (segmentCount == 0) {
            segmentCount++;
            return dir + "/segment-0";
        }
        return dir + "/segment-" + (segmentCount++);
    }

    @Override
    public String get(String key) {
        if (map.containsKey(key)) {
            return map.get(key);
        }
        return diskStorage.get(key);
    }
}
