package com.lsmt.storage;

import com.lsmt.model.SegmentEntry;

import java.io.IOException;

public interface Segment {

    SegmentIterator getIterator() throws IOException;

    String checkAndReturn(String key);

    void appendSegmentEntry(SegmentEntry segmentEntry) throws IOException;

    long length() throws IOException;

    String readStringAt(long offset, int len) throws IOException;

    void delete();

}
