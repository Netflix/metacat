/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


package com.netflix.metacat.common.server.spi;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Catalog config.
 */
// TODO: Move/refactor into connectors?
public final class MetacatCatalogConfig {
    private static final Splitter COMMA_LIST_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private final boolean includeViewsWithTables;
    private final List<String> schemaBlacklist;
    private final List<String> schemaWhitelist;
    private final int thriftPort;
    private final String type;

    private MetacatCatalogConfig(
        final String type,
        final boolean includeViewsWithTables,
        final List<String> schemaWhitelist,
        final List<String> schemaBlacklist,
        final int thriftPort) {
        this.type = type;
        this.includeViewsWithTables = includeViewsWithTables;
        this.schemaBlacklist = schemaBlacklist;
        this.schemaWhitelist = schemaWhitelist;
        this.thriftPort = thriftPort;
    }

    /**
     * Creates the config.
     *
     * @param type       type
     * @param properties properties
     * @return config
     */
    public static MetacatCatalogConfig createFromMapAndRemoveProperties(final String type,
                                                                        final Map<String, String> properties) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(type), "type is required");
        final String catalogType =
            properties.containsKey(Keys.CATALOG_TYPE) ? properties.remove(Keys.CATALOG_TYPE) : type;
        final List<String> schemaWhitelist = properties.containsKey(Keys.SCHEMA_WHITELIST)
            ? COMMA_LIST_SPLITTER.splitToList(properties.remove(Keys.SCHEMA_WHITELIST))
            : Collections.EMPTY_LIST;

        final List<String> schemaBlacklist = properties.containsKey(Keys.SCHEMA_BLACKLIST)
            ? COMMA_LIST_SPLITTER.splitToList(properties.remove(Keys.SCHEMA_BLACKLIST))
            : Collections.EMPTY_LIST;

        final boolean includeViewsWithTables = Boolean.parseBoolean(properties.remove(Keys.INCLUDE_VIEWS_WITH_TABLES));

        int thriftPort = 0;
        if (properties.containsKey(Keys.THRIFT_PORT)) {
            thriftPort = Integer.parseInt(properties.remove(Keys.THRIFT_PORT));
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

    /**
     * Properties in the catalog.
     */
    public static class Keys {
        /**
         * Catalog type.
         */
        public static final String CATALOG_TYPE = "metacat.type";
        /**
         * List views with tables.
         */
        public static final String INCLUDE_VIEWS_WITH_TABLES = "metacat.schema.list-views-with-tables";
        /**
         * Schemas that are black listed.
         */
        public static final String SCHEMA_BLACKLIST = "metacat.schema.blacklist";
        /**
         * Schemas that are white listed.
         */
        public static final String SCHEMA_WHITELIST = "metacat.schema.whitelist";
        /**
         * Thrift port.
         */
        public static final String THRIFT_PORT = "metacat.thrift.port";
    }
}
