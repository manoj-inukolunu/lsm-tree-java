package com.lsmt.storage;

import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


@Component("wal")
public class WriteAheadLog {

    FileWriter writer;
    File walFile;

    public WriteAheadLog() throws IOException {
        walFile = new File("/Users/manoj/kvdata/wal");
        if (walFile.exists()) {
            writer = new FileWriter(walFile, true);
        } else {
            writer = new FileWriter(walFile);
        }
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
