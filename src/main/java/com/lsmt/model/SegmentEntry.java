package com.lsmt.model;


import lombok.Data;

@Data
public class SegmentEntry {

    private String key;
    private String value;

    private int keyOffset;
    private int valueOffset;
    private int keyLen;

    public SegmentEntry(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public int getLen() {
        return valueOffset - keyOffset;
    }

}
