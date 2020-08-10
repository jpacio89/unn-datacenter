package com.unn.datacenter.models;

public class StandardResponse {
    private StatusResponse status;
    private String message;
    // private JsonElement data;
    private Object data;

    public StandardResponse(StatusResponse _status) {
        this.status = _status;
    }

    public StandardResponse(StatusResponse _status, String _message, Object _data) {
        this.status = _status;
        this.message = _message;
        this.data = _data;
    }
}
