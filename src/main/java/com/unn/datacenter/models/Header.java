package com.unn.datacenter.models;

import java.util.Arrays;

public class Header {
    String[] names;

    public Header() {

    }

    public String[] getNames() {
        return names;
    }

    public Header withNames(String[] names) {
        this.names = names;
        return this;
    }

    public Header withNames(String[] names, boolean copy) {
        if (!copy) {
            return this.withNames(names);
        }
        return this.withNames(Arrays.copyOf(names, names.length));
    }
}
