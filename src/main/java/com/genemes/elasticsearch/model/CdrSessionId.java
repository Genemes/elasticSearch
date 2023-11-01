package com.genemes.elasticsearch.model;

import org.springframework.data.annotation.Id;

public class CdrSessionId {

    @Id
    private String value;

    public CdrSessionId() {}

    public CdrSessionId(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "CdrSessionId: " + value;
    }
}
