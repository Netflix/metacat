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

package com.netflix.metacat.main.services.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePreEvent;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Database service implementation.
 */
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {
    private final CatalogService catalogService;
    private final ConnectorManager connectorManager;
    private final UserMetadataService userMetadataService;
    private final MetacatEventBus eventBus;
    private final ConverterUtil converterUtil;

    /**
     * Constructor.
     *
     * @param catalogService      catalog service
     * @param connectorManager    connector manager
     * @param userMetadataService user metadata service
     * @param eventBus            internal event bus
     * @param converterUtil       utility to convert to/from Dto to connector resources
     */
    public DatabaseServiceImpl(
        final CatalogService catalogService,
        final ConnectorManager connectorManager,
        final UserMetadataService userMetadataService,
        final MetacatEventBus eventBus,
        final ConverterUtil converterUtil
    ) {
        this.catalogService = catalogService;
        this.connectorManager = connectorManager;
        this.userMetadataService = userMetadataService;
        this.eventBus = eventBus;
        this.converterUtil = converterUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseDto create(final QualifiedName name, final DatabaseDto dto) {
        validate(name);
        log.info("Creating schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatCreateDatabasePreEvent(name, metacatRequestContext, this));
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        connectorManager.getDatabaseService(name.getCatalogName()).create(connectorRequestContext,
            converterUtil.fromDatabaseDto(dto));
        if (dto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for schema {}", name);
            userMetadataService.saveDefinitionMetadata(name, metacatRequestContext.getUserName(),
                Optional.of(dto.getDefinitionMetadata()), true);
        }
        final DatabaseDto createdDto = get(name, dto.getDefinitionMetadata() != null, true);
        eventBus.postAsync(new MetacatCreateDatabasePostEvent(name, metacatRequestContext, this, createdDto));
        return createdDto;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(final QualifiedName name, final DatabaseDto dto) {
        validate(name);
        log.info("Updating schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext, this));
        try {
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            connectorManager.getDatabaseService(name.getCatalogName())
                .update(connectorRequestContext, converterUtil.fromDatabaseDto(dto));
        } catch (UnsupportedOperationException ignored) {
        }
        if (dto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for schema {}", name);
            userMetadataService.saveDefinitionMetadata(name, metacatRequestContext.getUserName(),
                Optional.of(dto.getDefinitionMetadata()), true);
        }
        eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext, this));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseDto updateAndReturn(final QualifiedName name, final DatabaseDto dto) {
        update(name, dto);
        return get(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final QualifiedName name) {
        validate(name);
        log.info("Dropping schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final DatabaseDto dto = get(name, true, true);
        eventBus.postSync(new MetacatDeleteDatabasePreEvent(name, metacatRequestContext, this, dto));
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        connectorManager.getDatabaseService(name.getCatalogName()).delete(connectorRequestContext, name);

        // Delete definition metadata if it exists
        if (userMetadataService.getDefinitionMetadata(name).isPresent()) {
            log.info("Deleting user metadata for schema {}", name);
            userMetadataService.deleteDefinitionMetadatas(ImmutableList.of(name));
        }
        eventBus.postAsync(new MetacatDeleteDatabasePostEvent(name, metacatRequestContext, this, dto));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseDto get(final QualifiedName name) {
        return get(name, true, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseDto get(final QualifiedName name, final boolean includeUserMetadata,
        final boolean includeTableNames) {
        validate(name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final MetacatCatalogConfig config = connectorManager.getCatalogConfig(name.getCatalogName());
        final ConnectorDatabaseService service = connectorManager.getDatabaseService(name.getCatalogName());
        final ConnectorTableService tableService = connectorManager.getTableService(name.getCatalogName());
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);

        final DatabaseDto dto = converterUtil.toDatabaseDto(service.get(connectorRequestContext, name));
        dto.setType(connectorManager.getCatalogConfig(name).getType());
        if (includeTableNames) {
            final List<QualifiedName> tableNames = tableService
                .listNames(connectorRequestContext, name, null, null, null);
            List<QualifiedName> viewNames = Collections.emptyList();
            if (config.isIncludeViewsWithTables()) {
                // TODO JdbcMetadata returns ImmutableList.of() for views.  We should change it to fetch views.
                try {
                    viewNames = service.listViewNames(connectorRequestContext, name);
                } catch (UnsupportedOperationException ignored) {
                }
            }

            // Check to see if schema exists
            if (tableNames.isEmpty() && viewNames.isEmpty() && !exists(name)) {
                throw new DatabaseNotFoundException(name);
            }
            dto.setTables(
                Stream.concat(tableNames.stream(), viewNames.stream())
                    .map(QualifiedName::getTableName)
                    .sorted(String.CASE_INSENSITIVE_ORDER)
                    .collect(Collectors.toList())
            );
        }
        if (includeUserMetadata) {
            log.info("Populate user metadata for schema {}", name);
            userMetadataService.populateMetadata(dto);
        }

        return dto;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final QualifiedName name) {
        final CatalogDto catalogDto = catalogService.get(QualifiedName.ofCatalog(name.getCatalogName()));
        return catalogDto.getDatabases().contains(name.getDatabaseName());
    }

    private void validate(final QualifiedName name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkState(name.isDatabaseDefinition(), "name %s is not for a database", name);
    }
}
