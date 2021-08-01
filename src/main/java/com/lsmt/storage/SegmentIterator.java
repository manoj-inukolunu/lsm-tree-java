package com.lsmt.storage;

import com.lsmt.model.SegmentEntry;
import lombok.SneakyThrows;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SegmentIterator implements Iterator<SegmentEntry> {

    private final List<SegmentImpl.Entry> offsets;
    private int offsetIndex = 0;
    private final Segment segment;

    public SegmentIterator(SegmentImpl segment) {
        this.offsets = segment.getOffsets();
        this.segment = segment;
    }


    @Override
    public boolean hasNext() {
        return offsetIndex < offsets.size();
    }


    @Override
    @SneakyThrows
    public SegmentEntry next() {
        if (hasNext()) {
            long keyOffset = offsets.get(offsetIndex).getKeyOffset();
            int keyLen = offsets.get(offsetIndex).getLen();
            long valueOffset = offsets.get(offsetIndex).getValueOffset();
            int valueLen;
            if (offsetIndex == offsets.size() - 1) {
                valueLen = (int) (segment.length() - valueOffset);
            } else {
                valueLen = (int) (offsets.get(offsetIndex + 1).getKeyOffset() - offsets.get(offsetIndex).getValueOffset());
            }
            offsetIndex++;
            String key = segment.readStringAt(keyOffset, keyLen);
            String value = segment.readStringAt(valueOffset, valueLen);
            return new SegmentEntry(key, value);
        }
        throw new NoSuchElementException("Reached end");
    }
}
