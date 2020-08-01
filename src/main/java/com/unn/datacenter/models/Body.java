package com.unn.datacenter.models;

public class Body {
    Row[] rows;

    public Body() {

    }

    Body withRows(Row[] rows) {
        this.rows = rows;
        return this;
    }


}
