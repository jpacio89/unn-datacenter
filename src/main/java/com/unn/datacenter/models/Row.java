package com.unn.datacenter.models;

import java.util.Arrays;

public class Row {
    String[] values;

    public Row() {

    }

    public String[] getValues() {
        return values;
    }

    public Row withValues(String[] values) {
        this.values = values;
        return this;
    }

    public Row withValues(String[] values, boolean copy) {
        if (!copy) {
            return this.withValues(values);
        }
        return this.withValues(Arrays.copyOf(values, values.length));
    }
}
