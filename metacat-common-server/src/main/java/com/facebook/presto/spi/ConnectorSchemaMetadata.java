package com.facebook.presto.spi;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by amajumdar on 3/9/16.
 */
public class ConnectorSchemaMetadata {
    private String schemaName;
    private String uri;
    private Map<String, String> metadata;

    public ConnectorSchemaMetadata(String schemaName) {
        this(schemaName, null);
    }

    public ConnectorSchemaMetadata(String schemaName, String uri) {
        this(schemaName, uri, Maps.newHashMap());
    }

    public ConnectorSchemaMetadata(String schemaName, String uri, Map<String, String> metadata) {
        this.schemaName = schemaName;
        this.uri = uri;
        this.metadata = metadata;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getUri() {
        return uri;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}
