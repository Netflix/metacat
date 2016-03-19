package com.facebook.presto.exception;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;

/**
 * Created by amajumdar on 4/30/15.
 */
public class SchemaAlreadyExistsException extends PrestoException{
    private final String schemaName;
    public SchemaAlreadyExistsException(String schemaName) {
        this(schemaName, null);
    }

    public SchemaAlreadyExistsException(String schemaName, Throwable cause) {
        super(StandardErrorCode.ALREADY_EXISTS, String.format("Schema %s already exists.", schemaName), cause);
        this.schemaName = schemaName;
    }
}
