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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.main.connector.MetacatConnectorManager;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.SessionProvider;
import com.netflix.metacat.main.spi.MetacatCatalogConfig;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CatalogServiceImpl implements CatalogService {
    @Inject
    MetacatConnectorManager metacatConnectorManager;
    @Inject
    MetadataManager metadataManager;
    @Inject
    SessionProvider sessionProvider;
    @Inject
    UserMetadataService userMetadataService;

    @Nonnull
    @Override
    public CatalogDto get(@Nonnull QualifiedName name) {
        Session session = sessionProvider.getSession(name);

        MetacatCatalogConfig config = metacatConnectorManager.getCatalogConfig(name);

        CatalogDto result = new CatalogDto();
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
        Map<String, MetacatCatalogConfig> catalogs = metacatConnectorManager.getCatalogs();
        if (catalogs.isEmpty()) {
            throw new MetacatNotFoundException("Unable to locate any catalogs");
        }

        return catalogs.entrySet().stream()
                .map(entry -> new CatalogMappingDto(entry.getKey(), entry.getValue().getType()))
                .collect(Collectors.toList());
    }
}
