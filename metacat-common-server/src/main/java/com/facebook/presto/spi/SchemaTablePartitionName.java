package com.facebook.presto.spi;

/**
 * Created by amajumdar on 7/13/15.
 */
public class SchemaTablePartitionName {
    private final SchemaTableName tableName;
    private final String partitionId;

    public SchemaTablePartitionName(SchemaTableName tableName, String partitionId) {
        this.tableName = tableName;
        this.partitionId = partitionId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public SchemaTableName getTableName() {
        return tableName;
    }
}
