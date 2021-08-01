package com.lsmt.storage;

import com.lsmt.model.SegmentEntry;
import lombok.Data;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Component("inMemoryStorage")
@Data
public class InMemoryStorage implements Storage {


    @Value("${memtable.size:10}")
    public int size;

    private final TreeMap<String, String> memtable = new TreeMap<>();
    private final TreeSet<String> tombstoned = new TreeSet<>();
    private final ExecutorService service = Executors.newCachedThreadPool();
    private int segmentCount = 0;

    private final Storage diskStorage;
    private final WriteAheadLog writeAheadLog;

    public InMemoryStorage(Storage diskStorage, WriteAheadLog writeAheadLog) {
        this.diskStorage = diskStorage;
        this.writeAheadLog = writeAheadLog;
        for (String line : writeAheadLog.readLine()) {
            //TODO : Split by whitespace not ideal  this means keys and values should not have whitespaces
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
                SegmentImpl segment = SegmentImpl.create(getSegmentFileName("/Users/manoj/kvdata"));
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    segment.appendSegmentEntry(new SegmentEntry(entry.getKey(), entry.getValue()));
                }
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
        return dir + "/segment-" + (UUID.randomUUID());
    }

    private String getLocal(String key) {
        if (memtable.containsKey(key)) {
            return memtable.get(key);
        }
        return diskStorage.get(key);
    }

    @Override
    public String get(String key) {
        String value = getLocal(key);
        if (value != null && value.equals("##TOMBSTONED##")) {
            return null;
        }
        return value;
    }

    @Override
    public void delete(String key) {
        put(key, "##TOMBSTONED##");
    }

    @Override
    public void update(String key, String value) {
        put(key, value);
    }
}
