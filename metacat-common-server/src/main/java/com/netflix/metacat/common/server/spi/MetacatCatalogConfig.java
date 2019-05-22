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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.model.CatalogInfo;
import com.netflix.metacat.common.server.connectors.model.ClusterInfo;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Catalog config.
 */
@Getter
public final class MetacatCatalogConfig {
    private static final Splitter COMMA_LIST_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private final boolean includeViewsWithTables;
    private final List<String> schemaBlacklist;
    private final List<String> schemaWhitelist;
    private final int thriftPort;
    private final String type;
    private final String catalogName;
    private final ClusterInfo clusterInfo;

    private MetacatCatalogConfig(
        final String type,
        final String catalogName,
        final ClusterInfo clusterInfo,
        final boolean includeViewsWithTables,
        final List<String> schemaWhitelist,
        final List<String> schemaBlacklist,
        final int thriftPort) {
        this.type = type;
        this.catalogName = catalogName;
        this.clusterInfo = clusterInfo;
        this.includeViewsWithTables = includeViewsWithTables;
        this.schemaBlacklist = schemaBlacklist;
        this.schemaWhitelist = schemaWhitelist;
        this.thriftPort = thriftPort;
    }

    /**
     * Creates the config.
     *
     * @param type       type
     * @param catalogName catalog name
     * @param properties properties
     * @return config
     */
    public static MetacatCatalogConfig createFromMapAndRemoveProperties(
        final String type,
        final String catalogName,
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
        // Cluster information
        final String clusterName = properties.get(Keys.CLUSTER_NAME);
        final String clusterAccount = properties.get(Keys.CLUSTER_ACCOUNT);
        final String clusterAccountId = properties.get(Keys.CLUSTER_ACCOUNT_ID);
        final String clusterEnv = properties.get(Keys.CLUSTER_ENV);
        final String clusterRegion = properties.get(Keys.CLUSTER_REGION);
        final ClusterInfo clusterInfo =
            new ClusterInfo(clusterName, type, clusterAccount, clusterAccountId, clusterEnv, clusterRegion);

        return new MetacatCatalogConfig(catalogType, catalogName, clusterInfo, includeViewsWithTables, schemaWhitelist,
            schemaBlacklist, thriftPort);
    }

    public boolean isThriftInterfaceRequested() {
        return thriftPort != 0;
    }

    /**
     * Returns true if catalog is a proxy one.
     * @return Returns true if catalog is a proxy one.
     */
    public boolean isProxy() {
        return type.equalsIgnoreCase("proxy");
    }

    /**
     * Creates the catalog info.
     * @return catalog info
     */
    public CatalogInfo toCatalogInfo() {
        return CatalogInfo.builder().name(QualifiedName.ofCatalog(catalogName)).clusterInfo(clusterInfo).build();
    }

    /**
     * Properties in the catalog.
     */
    public static class Keys {
        /**
         * Catalog name. Usually catalog name is derived from the name of the catalog properties file.
         * One could also specify it in the properties under the property name <code>catalog.name</code>.
         * For example, if a catalog has two shards with catalog defined in two property files,
         * then we can have the <code>catalog.name</code> set to one name.
         */
        public static final String CATALOG_NAME = "catalog.name";
        /**
         * Connector type.
         */
        public static final String CONNECTOR_NAME = "connector.name";
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
        /**
         *  Cluster name.
         */
        public static final String CLUSTER_NAME = "metacat.cluster.name";
        /**
         *  Cluster account.
         */
        public static final String CLUSTER_ACCOUNT = "metacat.cluster.account";
        /**
         *  Cluster account id.
         */
        public static final String CLUSTER_ACCOUNT_ID = "metacat.cluster.account-id";
        /**
         *  Cluster region.
         */
        public static final String CLUSTER_REGION = "metacat.cluster.region";
        /**
         *  Cluster environment.
         */
        public static final String CLUSTER_ENV = "metacat.cluster.env";
    }
}
