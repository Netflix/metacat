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
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePreEvent;
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.spi.MetacatCatalogConfig;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.inject.Inject;
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
     * @param catalogService catalog service
     * @param connectorManager connector manager
     * @param userMetadataService user metadata service
     * @param eventBus internal event bus
     * @param converterUtil utility to convert to/from Dto to connector resources
     */
    @Inject
    public DatabaseServiceImpl(final CatalogService catalogService,
        final ConnectorManager connectorManager,
        final UserMetadataService userMetadataService, final MetacatEventBus eventBus,
        final ConverterUtil converterUtil) {
        this.catalogService = catalogService;
        this.connectorManager = connectorManager;
        this.userMetadataService = userMetadataService;
        this.eventBus = eventBus;
        this.converterUtil = converterUtil;
    }

    @Override
    public DatabaseDto create(@Nonnull final QualifiedName name, @Nonnull final DatabaseDto dto) {
        validate(name);
        log.info("Creating schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatCreateDatabasePreEvent(name, metacatRequestContext));
        final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
        connectorManager.getDatabaseService(name.getCatalogName()).create(connectorContext,
            converterUtil.fromDatabaseDto(dto));
        if (dto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for schema {}", name);
            userMetadataService.saveDefinitionMetadata(name, metacatRequestContext.getUserName(),
                    Optional.of(dto.getDefinitionMetadata()), true);
        }
        final DatabaseDto createdDto = get(name, dto.getDefinitionMetadata() != null);
        eventBus.postAsync(new MetacatCreateDatabasePostEvent(name, metacatRequestContext, createdDto));
        return createdDto;
    }

    @Override
    public void update(@Nonnull final QualifiedName name, @Nonnull final DatabaseDto dto) {
        validate(name);
        log.info("Updating schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext));
        try {
            final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
            connectorManager.getDatabaseService(name.getCatalogName())
                .update(connectorContext, converterUtil.fromDatabaseDto(dto));
        } catch (UnsupportedOperationException ignored) { }
        if (dto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for schema {}", name);
            userMetadataService.saveDefinitionMetadata(name, metacatRequestContext.getUserName(),
                Optional.of(dto.getDefinitionMetadata()), true);
        }
        eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext));
    }

    @Override
    public DatabaseDto updateAndReturn(@Nonnull final QualifiedName name, @Nonnull final DatabaseDto dto) {
        update(name, dto);
        return get(name);
    }

    @Override
    public void delete(@Nonnull final QualifiedName name) {
        validate(name);
        log.info("Dropping schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final DatabaseDto dto = get(name, true);
        eventBus.postSync(new MetacatDeleteDatabasePreEvent(name, metacatRequestContext, dto));
        final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
        connectorManager.getDatabaseService(name.getCatalogName()).delete(connectorContext, name);

        // Delete definition metadata if it exists
        if (userMetadataService.getDefinitionMetadata(name).isPresent()) {
            log.info("Deleting user metadata for schema {}", name);
            userMetadataService.deleteDefinitionMetadatas(ImmutableList.of(name));
        }
        eventBus.postAsync(new MetacatDeleteDatabasePostEvent(name, metacatRequestContext, dto));
    }

    @Override
    public DatabaseDto get(@Nonnull final QualifiedName name) {
        return get(name, true);
    }

    @Override
    public DatabaseDto get(@Nonnull final QualifiedName name, final boolean includeUserMetadata) {
        validate(name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final MetacatCatalogConfig config = connectorManager.getCatalogConfig(name.getCatalogName());
        final ConnectorDatabaseService service = connectorManager.getDatabaseService(name.getCatalogName());
        final ConnectorTableService tableService = connectorManager.getTableService(name.getCatalogName());
        final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
        final List<QualifiedName> tableNames = tableService.listNames(connectorContext, name, null, null, null);
        List<QualifiedName> viewNames = Collections.emptyList();
        if (config.isIncludeViewsWithTables()) {
            // TODO JdbcMetadata returns ImmutableList.of() for views.  We should change it to fetch views.
            try {
                viewNames = service.listViewNames(connectorContext, name);
            } catch (UnsupportedOperationException ignored) { }
        }

        // Check to see if schema exists
        if (tableNames.isEmpty() && viewNames.isEmpty() && !exists(name)) {
            throw new DatabaseNotFoundException(name);
        }

        final DatabaseDto dto = converterUtil.toDatabaseDto(service.get(connectorContext, name));
        dto.setType(connectorManager.getCatalogConfig(name).getType());
        dto.setTables(
            Stream.concat(tableNames.stream(), viewNames.stream())
                .map(QualifiedName::getTableName)
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .collect(Collectors.toList())
        );
        if (includeUserMetadata) {
            log.info("Populate user metadata for schema {}", name);
            userMetadataService.populateMetadata(dto);
        }

        return dto;
    }

    @Override
    public boolean exists(@Nonnull final QualifiedName name) {
        final CatalogDto catalogDto = catalogService.get(QualifiedName.ofCatalog(name.getCatalogName()));
        return catalogDto.getDatabases().contains(name.getDatabaseName());
    }

    private void validate(final QualifiedName name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkState(name.isDatabaseDefinition(), "name %s is not for a database", name);
    }
}
