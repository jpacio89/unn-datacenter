package com.unn.datacenter.models;

public class Body {
    Row[] rows;

    public Body() {

    }

    public Row[] getRows() {
        return this.rows;
    }

    public Body withRows(Row[] rows) {
        this.rows = rows;
        return this;
    }


}
