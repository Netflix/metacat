/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive;

import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;

/**
 * Hive base connector base service impl.
 *
 * @author zhenl
 */
public class HiveConnectorTableService implements ConnectorTableService {
    private final MetacatHiveClient metacatHiveClient;
    private final HiveConnectorInfoConverter hiveMetacatConverters;

    /**
     * Constructor.
     *
     * @param metacatHiveClient     hiveclient
     * @param hiveMetacatConverters converter
     */
    @Inject
    public HiveConnectorTableService(@Nonnull final MetacatHiveClient metacatHiveClient,
                                     @Nonnull final HiveConnectorInfoConverter hiveMetacatConverters) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
    }

    /**
     * getTable.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to get
     * @return table dto
     */
    @Override
    public TableInfo get(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        try {
            final Table table = metacatHiveClient.getTableByName(name.getDatabaseName(), name.getTableName());
            return hiveMetacatConverters.toTableInfo(name, table);
        } catch (TException exception) {
            throw new TableNotFoundException(name);
        }
    }

    /**
     * Create a table.
     *
     * @param requestContext The request context
     * @param tableInfo       The resource metadata
     * @throws TableAlreadyExistsException already exist exception
     */
    @Override
    public void create(@Nonnull final ConnectorContext requestContext, @Nonnull final TableInfo tableInfo) {
        try {
            metacatHiveClient.createTable(hiveMetacatConverters.fromTableInfo(tableInfo));
        } catch (TException exception) {
            throw new TableAlreadyExistsException(tableInfo.getName(), exception);
        }
    }

    /**
     * Delete a table with the given qualified name.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to delete
     */
    @Override
    public void delete(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        try {
            metacatHiveClient.dropTable(name.getDatabaseName(), name.getTableName());
        } catch (TException exception) {
            throw new TableNotFoundException(name, exception);
        }
    }

    /**
     * Update a resource with the given metadata.
     *
     * @param requestContext The request context
     * @param tableInfo       The resource metadata
     */
    @Override
    public void update(@Nonnull final ConnectorContext requestContext, @Nonnull final TableInfo tableInfo) {
        try {
            metacatHiveClient.alterTable(tableInfo.getName().getDatabaseName(),
                tableInfo.getName().getTableName(),
                hiveMetacatConverters.fromTableInfo(tableInfo));
        } catch (TException exception) {
            throw new TableNotFoundException(tableInfo.getName(), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
        @Nonnull final ConnectorContext requestContext,
        @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        try {
            List<QualifiedName> qualifiedNames = Lists.newArrayList();
            for (String tableName : metacatHiveClient.getAllTables(name.getDatabaseName())) {
                qualifiedNames.add(QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), tableName));
            }
            if (null != pageable && pageable.isPageable()) {
                final int limit = Math.min(pageable.getOffset() + pageable.getLimit(), qualifiedNames.size());
                qualifiedNames = (pageable.getOffset() > limit) ? Lists.newArrayList()
                    : qualifiedNames.subList(pageable.getOffset(), limit);
            }
            return  qualifiedNames;
        } catch (MetaException exception) {
            throw new DatabaseNotFoundException(name, exception);
        }
    }
}
