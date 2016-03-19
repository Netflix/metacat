package com.facebook.presto.exception;

import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;

/**
 * Created by amajumdar on 4/30/15.
 */
public class PartitionNotFoundException extends NotFoundException{
    private final SchemaTableName tableName;
    private final String partitionId;
    public PartitionNotFoundException(SchemaTableName tableName, String partitionId) {
        this(tableName, partitionId, null);
    }

    public PartitionNotFoundException(SchemaTableName tableName, String partitionId, Throwable cause) {
        super(String.format("Partition %s not found for table %s", tableName, partitionId==null?"": partitionId), cause);
        this.tableName = tableName;
        this.partitionId = partitionId;
    }
}
