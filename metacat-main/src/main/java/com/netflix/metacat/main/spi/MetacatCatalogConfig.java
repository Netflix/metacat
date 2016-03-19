package com.netflix.metacat.main.spi;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class MetacatCatalogConfig {
    private static final Splitter COMMA_LIST_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private final boolean includeViewsWithTables;
    private final List<String> schemaBlacklist;
    private final List<String> schemaWhitelist;
    private final int thriftPort;
    private final String type;

    private MetacatCatalogConfig(
            @Nonnull String type,
            boolean includeViewsWithTables,
            @Nonnull List<String> schemaWhitelist,
            @Nonnull List<String> schemaBlacklist,
            int thriftPort) {
        this.type = type;
        this.includeViewsWithTables = includeViewsWithTables;
        this.schemaBlacklist = schemaBlacklist;
        this.schemaWhitelist = schemaWhitelist;
        this.thriftPort = thriftPort;
    }

    public static MetacatCatalogConfig createFromMapAndRemoveProperties(String type, Map<String, String> properties) {
        checkArgument(!Strings.isNullOrEmpty(type), "type is required");
        String catalogType = properties.containsKey(Keys.CATALOG_TYPE) ? properties.remove(Keys.CATALOG_TYPE) : type;
        List<String> schemaWhitelist = properties.containsKey(Keys.SCHEMA_WHITELIST) ?
                COMMA_LIST_SPLITTER.splitToList(properties.remove(Keys.SCHEMA_WHITELIST)) :
                Collections.EMPTY_LIST;

        List<String> schemaBlacklist = properties.containsKey(Keys.SCHEMA_BLACKLIST) ?
                COMMA_LIST_SPLITTER.splitToList(properties.remove(Keys.SCHEMA_BLACKLIST)) :
                Collections.EMPTY_LIST;

        boolean includeViewsWithTables = Boolean.parseBoolean(properties.remove(Keys.INCLUDE_VIEWS_WITH_TABLES));

        int thriftPort = 0;
        if (properties.containsKey(Keys.THRIFT_PORT)) {
            thriftPort = Integer.valueOf(properties.remove(Keys.THRIFT_PORT));
        }

        return new MetacatCatalogConfig(catalogType, includeViewsWithTables, schemaWhitelist, schemaBlacklist,
                thriftPort);
    }

    public List<String> getSchemaBlacklist() {
        return schemaBlacklist;
    }

    public List<String> getSchemaWhitelist() {
        return schemaWhitelist;
    }

    public int getThriftPort() {
        return thriftPort;
    }

    public String getType() {
        return type;
    }

    public boolean isIncludeViewsWithTables() {
        return includeViewsWithTables;
    }

    public boolean isThriftInterfaceRequested() {
        return thriftPort != 0;
    }

    public static class Keys {
        public static final String CATALOG_TYPE = "metacat.type";
        public static final String INCLUDE_VIEWS_WITH_TABLES = "metacat.schema.list-views-with-tables";
        public static final String SCHEMA_BLACKLIST = "metacat.schema.blacklist";
        public static final String SCHEMA_WHITELIST = "metacat.schema.whitelist";
        public static final String THRIFT_PORT = "metacat.thrift.port";
    }
}
