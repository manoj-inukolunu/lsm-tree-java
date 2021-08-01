package com.lsmt.storage;

import com.lsmt.model.SegmentEntry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
@Component
public class SegmentImpl implements Segment {

    @Data
    @RequiredArgsConstructor
    @AllArgsConstructor
    class Entry {
        @NonNull
        long keyOffset;
        @NonNull
        long valueOffset;
        int keyLen;

        public int getLen() {
            return (int) (valueOffset - keyOffset);
        }
    }

    @Override
    public String toString() {
        return "Segment{" + "segmentName='" + segmentName + '\'' + '}';
    }

    private String segmentName;
    private List<Entry> offsets;
    private RandomAccessFile segmentFile;
    private RandomAccessFile offsetFile;
    private int offsetIndex;
    private FileWriter offsetFileWriter;

    public SegmentImpl(String name) {
        this.segmentName = name;
        readOffsets();
    }

    public static SegmentImpl create(String absoluteFilePath) throws IOException {
        Files.createFile(Path.of(absoluteFilePath));
        Files.createFile(Path.of(absoluteFilePath + "-offset"));
        return new SegmentImpl(absoluteFilePath);
    }

    public void readOffsets() {
        String offsetFileName = segmentName + "-offset";
        try {
            offsetFileWriter = new FileWriter(offsetFileName, true);
            offsetFile = new RandomAccessFile(offsetFileName, "r");
            segmentFile = new RandomAccessFile(segmentName, "rw");
            offsets = Files.readAllLines(Paths.get(offsetFileName)).stream().map(s -> {
                int keyOffset = Integer.parseInt(s.substring(0, s.indexOf(" ")));
                int valueOffset = Integer.parseInt(s.substring(s.indexOf(" ") + 1));
                return new Entry(keyOffset, valueOffset);
            }).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getLastValueLen() throws IOException {
        long len = segmentFile.length();
        return len - offsets.get(offsets.size() - 1).valueOffset;
    }

    private String getStringAtOffset(long offset, int len) throws IOException {
        segmentFile.seek(offset);
        byte[] data = new byte[len];
        segmentFile.read(data);
        return new String(data);
    }

    private String getStringAtOffset(long offset) throws IOException {
        segmentFile.seek(offset);
        List<Byte> list = new ArrayList<>();
        while (true) {
            try {
                list.add(segmentFile.readByte());
            } catch (EOFException e) {
                byte[] arr = new byte[list.size()];
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = list.get(i);
                }
                return new String(arr);
            }
        }
    }


    public SegmentEntry getNextEntry() throws IOException {
        Entry entry = offsets.get(offsetIndex);
        String key = getStringAtOffset(entry.getKeyOffset(), entry.getLen());
        long valueOffSet = entry.getValueOffset();
        if (offsetIndex == offsets.size() - 1) {
            String value = getStringAtOffset(valueOffSet);
            return new SegmentEntry(key, value);
        }
        String value = getStringAtOffset(entry.getValueOffset(), (int) (offsets.get(offsetIndex + 1)
                .getKeyOffset() - valueOffSet));
        return new SegmentEntry(key, value);
    }


    @Override
    public SegmentIterator getIterator() throws IOException {
        return new SegmentIterator(new SegmentImpl(segmentName));
    }

    @SneakyThrows
    public String checkAndReturn(String key) {
        int low = 0, high = offsets.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Entry midEntry = offsets.get(mid);
            String midString = getStringAtOffset(midEntry.getKeyOffset(), midEntry.getLen());
            int compared = key.compareTo(midString);
            if (compared == 0) {
                long valueOffset = midEntry.getValueOffset();
                if (mid + 1 >= offsets.size()) {
                    return getStringAtOffset(valueOffset);
                }
                Entry nextOffsetEntry = offsets.get(mid + 1);
                return getStringAtOffset(valueOffset, (int) (nextOffsetEntry.getKeyOffset() - valueOffset));
            } else if (compared > 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return null;
    }

    @Override
    public void appendSegmentEntry(SegmentEntry segmentEntry) throws IOException {
        int keyLength = segmentEntry.getKey().length();
        offsets.add(new Entry(segmentFile.length(), segmentFile.length() + keyLength, keyLength));
        offsetFileWriter.append(String.valueOf(segmentFile.length())).append(" ")
                .append(String.valueOf(segmentFile.length() + keyLength)).append("\r\n");
        segmentFile.writeBytes(segmentEntry.getKey());
        segmentFile.writeBytes(segmentEntry.getValue());
        offsetFileWriter.flush();
    }

    @Override
    public long length() throws IOException {
        return segmentFile.length();
    }

    @Override
    public String readStringAt(long offset, int len) throws IOException {
        byte[] arr = new byte[len];
        segmentFile.seek(offset);
        segmentFile.read(arr);
        return new String(arr);
    }

    @Override
    public void delete() {
        new File(segmentName).delete();
        new File(segmentName + "-offset").delete();
    }

}
