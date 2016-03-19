package com.facebook.presto.exception;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;

/**
 * Created by amajumdar on 4/30/15.
 */
public class PartitionAlreadyExistsException extends PrestoException{
    private final SchemaTableName tableName;
    private final String partitionId;
    public PartitionAlreadyExistsException(SchemaTableName tableName, String partitionId) {
        this(tableName, partitionId, null);
    }

    public PartitionAlreadyExistsException(SchemaTableName tableName, String partitionId, Throwable cause) {
        super(StandardErrorCode.ALREADY_EXISTS, String.format("Partition %s already exists for table %s", tableName, partitionId==null?"": partitionId), cause);
        this.tableName = tableName;
        this.partitionId = partitionId;
    }
}
