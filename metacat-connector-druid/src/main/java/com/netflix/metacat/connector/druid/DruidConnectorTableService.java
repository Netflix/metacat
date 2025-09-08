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

package com.netflix.metacat.connector.druid;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetadataException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.druid.converter.DataSource;
import com.netflix.metacat.connector.druid.converter.DruidConnectorInfoConverter;
import com.netflix.metacat.connector.druid.converter.DruidConverterUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;

/**
 * Druid Connector Table Service, which manages druid data source.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Slf4j
public class  DruidConnectorTableService implements ConnectorTableService {
    private final MetacatDruidClient druidClient;
    private final DruidConnectorInfoConverter druidConnectorInfoConverter;
    /**
     * Constructor.
     *
     * @param druidClient druid client
     * @param druidConnectorInfoConverter druid infor object converter
     */
    public DruidConnectorTableService(
        final MetacatDruidClient druidClient,
        final DruidConnectorInfoConverter druidConnectorInfoConverter
    ) {
        this.druidClient = druidClient;
        this.druidConnectorInfoConverter = druidConnectorInfoConverter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableInfo get(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName name) {
        log.debug("Get table metadata for qualified name {} for request {}", name, context);
        try {
            final ObjectNode node = this.druidClient.getLatestDataByName(name.getTableName());
            final DataSource dataSource = DruidConverterUtil.getDatasourceFromLatestSegmentJsonObject(node);
            return this.druidConnectorInfoConverter.getTableInfoFromDatasource(dataSource);
        } catch (MetacatException e) {
            log.error(String.format("Table %s not found.", name), e);
            throw new TableNotFoundException(name);
        } catch (HttpClientErrorException e) {
            log.error(String.format("Failed getting table %s.", name), e);
            if (HttpStatus.NOT_FOUND.equals(e.getStatusCode())) {
                throw new TableNotFoundException(name);
            } else {
                throw new InvalidMetadataException(String.format("Invalid table %s. %s", name, e.getMessage()));
            }
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
        final ConnectorRequestContext requestContext,
        final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        try {
            final List<QualifiedName> qualifiedNames = Lists.newArrayList();
            final String tableFilter = (prefix != null && prefix.isTableDefinition()) ? prefix.getTableName() : null;
            for (String tableName : this.druidClient.getAllDataSources()) {
                final QualifiedName qualifiedName =
                    QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), tableName);
                if (tableFilter == null || tableName.startsWith(tableFilter)) {
                    qualifiedNames.add(qualifiedName);
                }
            }
            if (sort != null) {
                ConnectorUtils.sort(qualifiedNames, sort, Comparator.comparing(QualifiedName::toString));
            }
            return ConnectorUtils.paginate(qualifiedNames, pageable);
        } catch (Exception exception) {
            throw new ConnectorException(String.format("Failed listNames druid table %s", name), exception);
        }
    }
}
