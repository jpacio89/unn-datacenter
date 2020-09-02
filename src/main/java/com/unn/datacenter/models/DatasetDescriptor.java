package com.unn.datacenter.models;

public class DatasetDescriptor {
    public class Layer {
        public static final int INPUT = 0;
        public static final int OUTPUT = -1;
    }
    String[] uniques;
    String key;
    String description;
    String namespace;
    String[] upstreamDependencies;
    String[] downstreamDependencies;
    int layer;
    Header header;

    public DatasetDescriptor() {
        this.header = new Header();
    }

    public String[] getUpstreamDependencies() {
        return upstreamDependencies;
    }

    public DatasetDescriptor withUpstreamDependencies(String[] upstreamDependencies) {
        this.upstreamDependencies = upstreamDependencies;
        return this;
    }

    public String[] getDownstreamDependencies() {
        return upstreamDependencies;
    }

    public DatasetDescriptor withDownstreamDependencies(String[] downstreamDependencies) {
        this.downstreamDependencies = downstreamDependencies;
        return this;
    }

    public String[] getUniques() {
        return uniques;
    }

    public DatasetDescriptor withUniques(String[] uniques) {
        this.uniques = uniques;
        return this;
    }

    public String getKey() {
        return key;
    }

    public DatasetDescriptor withKey(String key) {
        this.key = key;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DatasetDescriptor withDescription(String description) {
        this.description = description;
        return this;
    }

    public String getNamespace() {
        return namespace;
    }

    public DatasetDescriptor withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public int getLayer() {
        return layer;
    }

    public DatasetDescriptor withLayer(int layer) {
        this.layer = layer;
        return this;
    }

    public Header getHeader() {
        return this.header;
    }

    public DatasetDescriptor withHeader(Header header) {
        this.header = header;
        return this;
    }

    public void setUniques(String[] uniques) {
        this.uniques = uniques;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setUpstreamDependencies(String[] upstreamDependencies) {
        this.upstreamDependencies = upstreamDependencies;
    }

    public void setDownstreamDependencies(String[] downstreamDependencies) {
        this.downstreamDependencies = downstreamDependencies;
    }

    public void setLayer(int layer) {
        this.layer = layer;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public boolean isAnnotated() {
        return this.downstreamDependencies == null;
    }
}
