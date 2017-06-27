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

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.CreateCatalogDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePreEvent;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Catalog service implementation.
 */
public class CatalogServiceImpl implements CatalogService {
    private final ConnectorManager connectorManager;
    private final UserMetadataService userMetadataService;
    private final MetacatEventBus eventBus;
    private final ConverterUtil converterUtil;

    /**
     * Constructor.
     *
     * @param connectorManager    connector manager
     * @param userMetadataService user metadata service
     * @param eventBus            Internal event bus
     * @param converterUtil       utility to convert to/from Dto to connector resources
     */
    public CatalogServiceImpl(
        final ConnectorManager connectorManager,
        final UserMetadataService userMetadataService,
        final MetacatEventBus eventBus,
        final ConverterUtil converterUtil
    ) {
        this.connectorManager = connectorManager;
        this.userMetadataService = userMetadataService;
        this.eventBus = eventBus;
        this.converterUtil = converterUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public CatalogDto get(final QualifiedName name) {
        final MetacatCatalogConfig config = connectorManager.getCatalogConfig(name);

        final CatalogDto result = new CatalogDto();
        result.setName(name);
        result.setType(config.getType());
        final ConnectorRequestContext context = converterUtil.toConnectorContext(MetacatContextManager.getContext());
        result.setDatabases(
            connectorManager.getDatabaseService(name.getCatalogName()).listNames(context, name, null, null, null)
                .stream().map(QualifiedName::getDatabaseName)
                .filter(s -> config.getSchemaBlacklist().isEmpty() || !config.getSchemaBlacklist().contains(s))
                .filter(s -> config.getSchemaWhitelist().isEmpty() || config.getSchemaWhitelist().contains(s))
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .collect(Collectors.toList())
        );
        userMetadataService.populateMetadata(result);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<CatalogMappingDto> getCatalogNames() {
        final Map<String, MetacatCatalogConfig> catalogs = connectorManager.getCatalogs();
        if (catalogs.isEmpty()) {
            throw new MetacatNotFoundException("Unable to locate any catalogs");
        }

        return catalogs.entrySet().stream()
            .map(entry -> new CatalogMappingDto(entry.getKey(), entry.getValue().getType()))
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(final QualifiedName name, final CreateCatalogDto createCatalogDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext, this));
        connectorManager.getCatalogConfig(name);
        userMetadataService.saveMetadata(metacatRequestContext.getUserName(), createCatalogDto, true);
        eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext, this));
    }
}
