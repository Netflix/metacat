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

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.connector.hive.converters.HiveMetacatConverters;
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
    private final HiveMetacatConverters hiveMetacatConverters;

    /**
     * Constructor.
     *
     * @param metacatHiveClient     hiveclient
     * @param hiveMetacatConverters converter
     */
    @Inject
    public HiveConnectorTableService(@Nonnull final MetacatHiveClient metacatHiveClient,
                                     @Nonnull final HiveMetacatConverters hiveMetacatConverters) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * getTable.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to get
     * @return table dto
     */
    public TableDto get(@Nonnull final MetacatRequestContext requestContext, @Nonnull final QualifiedName name) {
        try {
            final Table table = metacatHiveClient.getTableByName(name.getDatabaseName(), name.getTableName());
            return hiveMetacatConverters.hiveToMetacatTable(name, table);
        } catch (TException exception) {
            throw new TableNotFoundException(name);
        }
    }

    /**
     * Create a table.
     *
     * @param requestContext The request context
     * @param tableDto       The resource metadata
     * @throws TableAlreadyExistsException already exist exception
     */
    @Override
    public void create(@Nonnull final MetacatRequestContext requestContext, @Nonnull final TableDto tableDto) {
        try {
            metacatHiveClient.createTable(hiveMetacatConverters.metacatToHiveTable(tableDto));
        } catch (TException exception) {
            throw new TableAlreadyExistsException(tableDto.getName(), exception);
        }
    }

    /**
     * Delete a table with the given qualified name.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to delete
     */
    public void delete(@Nonnull final MetacatRequestContext requestContext, @Nonnull final QualifiedName name) {
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
     * @param tableDto       The resource metadata
     */
    public void update(@Nonnull final MetacatRequestContext requestContext, @Nonnull final TableDto tableDto) {
        try {
            metacatHiveClient.alterTable(tableDto.getName().getDatabaseName(),
                tableDto.getName().getTableName(),
                hiveMetacatConverters.metacatToHiveTable(tableDto));
        } catch (TException exception) {
            throw new TableNotFoundException(tableDto.getName(), exception);
        }
    }
}
