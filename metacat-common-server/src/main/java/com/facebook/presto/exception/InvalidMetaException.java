package com.facebook.presto.exception;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;

/**
 * Created by amajumdar on 5/11/15.
 */
public class InvalidMetaException extends PrestoException {
    private SchemaTableName tableName;
    private String partitionId;

    public InvalidMetaException(SchemaTableName tableName, Throwable cause) {
        super(StandardErrorCode.USER_ERROR
                , String.format("Invalid metadata for %s.", tableName)
                , cause);
        this.tableName = tableName;
    }

    public InvalidMetaException(SchemaTableName tableName, String partitionId, Throwable cause) {
        super(StandardErrorCode.USER_ERROR
                , String.format("Invalid metadata for %s for partition %s.", tableName, partitionId)
                , cause);
        this.tableName = tableName;
        this.partitionId = partitionId;
    }

    public InvalidMetaException(String message, Throwable cause) {
        super(StandardErrorCode.USER_ERROR
                , message
                , cause);
    }
}
