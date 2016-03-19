package com.netflix.metacat.common.json;

public class MetacatJsonException extends RuntimeException {
    public MetacatJsonException(String s) {
        super(s);
    }

    protected MetacatJsonException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public MetacatJsonException(Throwable cause) {
        super(cause);
    }

    public MetacatJsonException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetacatJsonException() {
        super();
    }
}
