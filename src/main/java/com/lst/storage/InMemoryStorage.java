package com.lst.storage;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Component("inMemoryStorage")
public class InMemoryStorage implements Storage {


    @Value("${memtable.size:10}")
    public int size;

    private final TreeMap<String, String> memtable = new TreeMap<>();
    private final ExecutorService service = Executors.newCachedThreadPool();
    private int segmentCount = 0;

    private final Storage diskStorage;
    private final WriteAheadLog writeAheadLog;

    public InMemoryStorage(Storage diskStorage, WriteAheadLog writeAheadLog) {
        this.diskStorage = diskStorage;
        this.writeAheadLog = writeAheadLog;
        for (String line : writeAheadLog.readLine()) {
            String[] data = line.split(" ");
            memtable.put(data[0], data[1]);
        }
    }


    @SneakyThrows
    @Override
    public void put(String key, String value) {
        if (memtable.size() >= size) {
            flushToDisk(memtable);
            writeAheadLog.delete();
            writeAheadLog.createNew();
            memtable.clear();
        }
        writeAheadLog.append(key, value);
        memtable.put(key, value);
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
        if (memtable.containsKey(key)) {
            return memtable.get(key);
        }
        return diskStorage.get(key);
    }
}
