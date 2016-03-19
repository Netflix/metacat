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
