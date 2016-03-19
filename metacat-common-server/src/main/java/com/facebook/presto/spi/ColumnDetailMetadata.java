package com.facebook.presto.spi;

import com.facebook.presto.spi.type.Type;

/**
 * Created by amajumdar on 9/28/15.
 */
public class ColumnDetailMetadata extends ColumnMetadata {
    private final String sourceType;
    private final Integer size;
    private final Boolean isNullable;
    private final String defaultValue;
    private final Boolean isSortKey;
    private final Boolean isIndexKey;

    public ColumnDetailMetadata(String name, Type type, boolean partitionKey, String sourceType) {
        this(name , type, partitionKey, null, false, sourceType, null, null, null, null, null);
    }

    public ColumnDetailMetadata(String name, Type type, boolean partitionKey, String comment, boolean hidden
            , String sourceType) {
        this(name , type, partitionKey, comment, hidden, sourceType, null, null, null, null, null);
    }

    public ColumnDetailMetadata(String name, Type type, boolean partitionKey, String comment, boolean hidden
            , String sourceType, Integer size, Boolean isNullable
            , String defaultValue, Boolean isSortKey, Boolean isIndexKey) {
        super(name, type, partitionKey, comment, hidden);
        this.sourceType = sourceType;
        this.size = size;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.isSortKey = isSortKey;
        this.isIndexKey = isIndexKey;
    }

    public String getSourceType() {
        return sourceType;
    }

    public Boolean getIsNullable() {
        return isNullable;
    }

    public Integer getSize() {
        return size;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Boolean getIsSortKey() {
        return isSortKey;
    }

    public Boolean getIsIndexKey() {
        return isIndexKey;
    }
}
