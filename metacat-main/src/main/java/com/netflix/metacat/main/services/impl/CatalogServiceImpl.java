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
import com.google.inject.Inject;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.CreateCatalogDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePreEvent;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.connector.MetacatConnectorManager;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.SessionProvider;
import com.netflix.metacat.main.spi.MetacatCatalogConfig;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Catalog service implementation.
 */
public class CatalogServiceImpl implements CatalogService {
    private final MetacatConnectorManager metacatConnectorManager;
    private final MetadataManager metadataManager;
    private final SessionProvider sessionProvider;
    private final UserMetadataService userMetadataService;
    private final MetacatEventBus eventBus;

    /**
     * Constructor.
     * @param metacatConnectorManager connector manager
     * @param metadataManager metadata manager
     * @param sessionProvider session provider
     * @param userMetadataService user metadata service
     * @param eventBus Internal event bus
     */
    @Inject
    public CatalogServiceImpl(final MetacatConnectorManager metacatConnectorManager,
        final MetadataManager metadataManager, final SessionProvider sessionProvider,
        final UserMetadataService userMetadataService, final MetacatEventBus eventBus) {
        this.metacatConnectorManager = metacatConnectorManager;
        this.metadataManager = metadataManager;
        this.sessionProvider = sessionProvider;
        this.userMetadataService = userMetadataService;
        this.eventBus = eventBus;
    }

    @Nonnull
    @Override
    public CatalogDto get(@Nonnull final QualifiedName name) {
        final Session session = sessionProvider.getSession(name);
        final MetacatCatalogConfig config = metacatConnectorManager.getCatalogConfig(name);

        final CatalogDto result = new CatalogDto();
        result.setName(name);
        result.setType(config.getType());
        result.setDatabases(metadataManager.listSchemaNames(session, name.getCatalogName())
            .stream()
            .filter(s -> config.getSchemaBlacklist().isEmpty() || !config.getSchemaBlacklist().contains(s))
            .filter(s -> config.getSchemaWhitelist().isEmpty() || config.getSchemaWhitelist().contains(s))
            .sorted(String.CASE_INSENSITIVE_ORDER)
            .collect(Collectors.toList())
        );
        userMetadataService.populateMetadata(result);
        return result;
    }

    @Nonnull
    @Override
    public List<CatalogMappingDto> getCatalogNames() {
        final Map<String, MetacatCatalogConfig> catalogs = metacatConnectorManager.getCatalogs();
        if (catalogs.isEmpty()) {
            throw new MetacatNotFoundException("Unable to locate any catalogs");
        }

        return catalogs.entrySet().stream()
            .map(entry -> new CatalogMappingDto(entry.getKey(), entry.getValue().getType()))
            .collect(Collectors.toList());
    }

    @Override
    public void update(@Nonnull final QualifiedName name, @Nonnull final CreateCatalogDto createCatalogDto) {
        final Session session = sessionProvider.getSession(name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext));
        userMetadataService.saveMetadata(session.getUser(), createCatalogDto, true);
        eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext));
    }
}
