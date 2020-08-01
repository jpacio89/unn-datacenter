package com.unn.datacenter.models;

public class DatasetDescriptor {
    String[] uniques;
    String primary;
    String description;
    String name;

    public DatasetDescriptor() {

    }

    public String[] getUniques() {
        return uniques;
    }

    public DatasetDescriptor withUniques(String[] uniques) {
        this.uniques = uniques;
        return this;
    }

    public String getPrimary() {
        return primary;
    }

    public DatasetDescriptor withPrimary(String primary) {
        this.primary = primary;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DatasetDescriptor withDescription(String description) {
        this.description = description;
        return this;
    }

    public String getName() {
        return name;
    }

    public DatasetDescriptor withName(String name) {
        this.name = name;
        return this;
    }
}
