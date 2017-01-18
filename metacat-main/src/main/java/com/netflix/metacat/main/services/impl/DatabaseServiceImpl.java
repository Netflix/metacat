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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.spi.ConnectorSchemaMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePreEvent;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.connector.MetacatConnectorManager;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.SessionProvider;
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
    private final MetacatConnectorManager metacatConnectorManager;
    private final MetadataManager metadataManager;
    private final SessionProvider sessionProvider;
    private final UserMetadataService userMetadataService;
    private final MetacatEventBus eventBus;

    /**
     * Constructor.
     * @param catalogService catalog service
     * @param metacatConnectorManager connector manager
     * @param metadataManager metadata manager
     * @param sessionProvider session provider
     * @param userMetadataService user metadata service
     * @param eventBus internal event bus
     */
    @Inject
    public DatabaseServiceImpl(final CatalogService catalogService,
        final MetacatConnectorManager metacatConnectorManager,
        final MetadataManager metadataManager, final SessionProvider sessionProvider,
        final UserMetadataService userMetadataService, final MetacatEventBus eventBus) {
        this.catalogService = catalogService;
        this.metacatConnectorManager = metacatConnectorManager;
        this.metadataManager = metadataManager;
        this.sessionProvider = sessionProvider;
        this.userMetadataService = userMetadataService;
        this.eventBus = eventBus;
    }

    @Override
    public DatabaseDto create(@Nonnull final QualifiedName name, @Nonnull final DatabaseDto dto) {
        final Session session = validateAndGetSession(name);
        log.info("Creating schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatCreateDatabasePreEvent(name, metacatRequestContext));
        metadataManager.createSchema(session, new ConnectorSchemaMetadata(name.getDatabaseName()));
        if (dto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for schema {}", name);
            userMetadataService
                .saveDefinitionMetadata(name, session.getUser(), Optional.of(dto.getDefinitionMetadata()), true);
        }
        final DatabaseDto createdDto = get(name, dto.getDefinitionMetadata() != null);
        eventBus.postAsync(new MetacatCreateDatabasePostEvent(name, metacatRequestContext, createdDto));
        return createdDto;
    }

    @Override
    public void update(@Nonnull final QualifiedName name, @Nonnull final DatabaseDto dto) {
        final Session session = validateAndGetSession(name);
        log.info("Updating schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext));
        try {
            metadataManager.updateSchema(session, new ConnectorSchemaMetadata(name.getDatabaseName()));
        } catch (PrestoException e) {
            if (e.getErrorCode() != StandardErrorCode.NOT_SUPPORTED.toErrorCode()) {
                throw e;
            }
        }
        if (dto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for schema {}", name);
            userMetadataService
                .saveDefinitionMetadata(name, session.getUser(), Optional.of(dto.getDefinitionMetadata()), true);
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
        final Session session = validateAndGetSession(name);
        log.info("Dropping schema {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final DatabaseDto dto = get(name, true);
        eventBus.postSync(new MetacatDeleteDatabasePreEvent(name, metacatRequestContext, dto));
        metadataManager.dropSchema(session);

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
        final Session session = validateAndGetSession(name);
        final MetacatCatalogConfig config = metacatConnectorManager.getCatalogConfig(name.getCatalogName());

        final QualifiedTablePrefix spec = new QualifiedTablePrefix(name.getCatalogName(), name.getDatabaseName());
        final List<QualifiedTableName> tableNames = metadataManager.listTables(session, spec);
        List<QualifiedTableName> viewNames = Collections.emptyList();
        if (config.isIncludeViewsWithTables()) {
            // TODO JdbcMetadata returns ImmutableList.of() for views.  We should change it to fetch views.
            viewNames = metadataManager.listViews(session, spec);
        }

        // Check to see if schema exists
        if (tableNames.isEmpty() && viewNames.isEmpty() && !exists(name)) {
            throw new SchemaNotFoundException(name.getDatabaseName());
        }

        final ConnectorSchemaMetadata schema = metadataManager.getSchema(session);

        final DatabaseDto dto = new DatabaseDto();
        dto.setType(metacatConnectorManager.getCatalogConfig(name).getType());
        dto.setName(name);
        dto.setUri(schema.getUri());
        dto.setMetadata(schema.getMetadata());
        dto.setTables(
            Stream.concat(tableNames.stream(), viewNames.stream())
                .map(QualifiedTableName::getTableName)
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

    private Session validateAndGetSession(final QualifiedName name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkState(name.isDatabaseDefinition(), "name %s is not for a database", name);

        return sessionProvider.getSession(name);
    }
}
