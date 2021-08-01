package com.lsmt.storage;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class Segment {

    @Data
    @RequiredArgsConstructor
    class Entry {
        @NonNull
        int keyOffset;
        @NonNull
        int valueOffset;
        int keyLen;

        public int getLen() {
            return valueOffset - keyOffset;
        }
    }

    @Override
    public String toString() {
        return "Segment{" +
                "segmentName='" + segmentName + '\'' +
                '}';
    }

    private String first;
    private String last;
    private String segmentName;
    private List<Entry> offsets;
    private RandomAccessFile file;

    public Segment(String name) {
        String offsetFileName = name + "-offset";
        try {
            file = new RandomAccessFile(name, "r");
            offsets = Files.readAllLines(Paths.get(offsetFileName)).stream().map(s -> {
                int keyOffset = Integer.parseInt(s.substring(0, s.indexOf(" ")));
                int valueOffset = Integer.parseInt(s.substring(s.indexOf(" ") + 1));
                return new Entry(keyOffset, valueOffset);
            }).collect(Collectors.toList());
            first = getStringAtOffset(0, offsets.get(0).getLen());
            last = getStringAtOffset(offsets.get(offsets.size() - 1).getKeyOffset(), offsets.get(offsets.size() - 1).getLen());
            this.segmentName = name;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getStringAtOffset(int offset, int len) throws IOException {
        file.seek(offset);
        byte[] data = new byte[len];
        file.read(data);
        return new String(data);
    }

    private String getStringAtOffset(int offset) throws IOException {
        file.seek(offset);
        List<Byte> list = new ArrayList<>();
        while (true) {
            try {
                list.add(file.readByte());
            } catch (EOFException e) {
                byte[] arr = new byte[list.size()];
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = list.get(i);
                }
                return new String(arr);
            }
        }
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
                int valueOffset = midEntry.getValueOffset();
                if (mid + 1 >= offsets.size()) {
                    return getStringAtOffset(valueOffset);
                }
                Entry nextOffsetEntry = offsets.get(mid + 1);
                return getStringAtOffset(valueOffset, nextOffsetEntry.getKeyOffset() - valueOffset);
            } else if (compared > 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return null;
    }

}
