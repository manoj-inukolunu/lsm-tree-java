package com.lst.storage;

import lombok.SneakyThrows;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class WriteAheadLog {

    File walFile = new File("/Users/manoj/kvdata/wal");
    FileWriter writer = new FileWriter(walFile);

    public WriteAheadLog() throws IOException {
    }

    public void append(String key, String value) throws IOException {
        try {
            writer.append(key).append(" ").append(value).append("\r\n");
            writer.flush();
        } catch (Exception e) {
            writer.close();
        }
    }

    public void delete() {
        walFile.delete();
    }

    @SneakyThrows
    public void createNew() {
        walFile.createNewFile();
        writer = new FileWriter(walFile);
    }

    @SneakyThrows
    public List<String> readLine() {
        return Files.readAllLines(Paths.get("/Users/manoj/kvdata/wal"));
    }
}
