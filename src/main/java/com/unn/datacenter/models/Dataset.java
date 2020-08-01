package com.unn.datacenter.models;

public class Dataset {
    DatasetDescriptor descriptor;
    Header header;
    Body body;

    public Dataset() {

    }

    public Dataset withDescriptor(DatasetDescriptor descriptor) {
        this.descriptor = descriptor;
        return this;
    }

    public Dataset withHeader(Header header) {
        this.header = header;
        return this;
    }

    public Dataset withBody(Body body) {
        this.body = body;
        return this;
    }
}
