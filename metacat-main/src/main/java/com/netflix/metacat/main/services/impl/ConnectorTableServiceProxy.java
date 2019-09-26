/*
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.metacat.main.services.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.services.GetTableNamesServiceParameters;
import com.netflix.metacat.main.services.GetTableServiceParameters;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles calls to the connector table service.
 */
@Slf4j
@CacheConfig(cacheNames = "metacat")
public class ConnectorTableServiceProxy {
    private final ConnectorManager connectorManager;
    private final ConverterUtil converterUtil;

    /**
     * Constructor.
     *
     * @param connectorManager    connector manager
     * @param converterUtil       utility to convert to/from Dto to connector resources
     */
    public ConnectorTableServiceProxy(
        final ConnectorManager connectorManager,
        final ConverterUtil converterUtil
    ) {
        this.connectorManager = connectorManager;
        this.converterUtil = converterUtil;
    }

    /**
     * Calls the connector table service create method.
     * @param name table name
     * @param tableInfo table object
     */
    public void create(final QualifiedName name, final TableInfo tableInfo) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(name);
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        service.create(connectorRequestContext, tableInfo);
    }

    /**
     * Calls the connector table service delete method.
     * @param name table name
     */
    @CacheEvict(key = "'table.' + #name")
    public void delete(final QualifiedName name) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(name);

        log.info("Drop table {}", name);
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        service.delete(connectorRequestContext, name);
    }


    /**
     *
     * Returns table if <code>useCache</code> is true and object exists in the cache. If <code>useCache</code> is false
     * or object does not exists in the cache, it is retrieved from the store.
     * @param name table name
     * @param getTableServiceParameters  get table parameters
     * @param useCache true, if table can be retrieved from cache
     * @return table dto
     */
    @Cacheable(key = "'table.' + #name", condition = "#useCache")
    public TableInfo get(final QualifiedName name,
                         final GetTableServiceParameters getTableServiceParameters,
                         final boolean useCache) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        connectorRequestContext.setIncludeMetadata(getTableServiceParameters.isIncludeMetadataFromConnector());
        final ConnectorTableService service = connectorManager.getTableService(name);
        return service.get(connectorRequestContext, name);
    }

    /**
     * Calls the connector table service rename method.
     * @param oldName old table name
     * @param newName new table name
     * @param isMView true, if the object is a view
     */
    @CacheEvict(key = "'table.' + #oldName")
    public void rename(
        final QualifiedName oldName,
        final QualifiedName newName,
        final boolean isMView
    ) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(oldName);

        try {
            log.info("Renaming {} {} to {}", isMView ? "view" : "table", oldName, newName);
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            service.rename(connectorRequestContext, oldName, newName);
        } catch (UnsupportedOperationException ignored) {
        }
    }

    /**
     * Calls the connector table service update method.
     * @param name table name
     * @param tableInfo table object
     * @return true if errors after this should be ignored.
     */
    @CacheEvict(key = "'table.' + #name")
    public boolean update(final QualifiedName name, final TableInfo tableInfo) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(name);
        boolean result = false;
        try {
            log.info("Updating table {}", name);
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            service.update(connectorRequestContext, tableInfo);
            result = connectorRequestContext.isIgnoreErrorsAfterUpdate();
        } catch (UnsupportedOperationException ignored) {
            //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata.
            log.debug("Catalog {} does not support the table update operation.", name.getCatalogName());
        }
        return result;
    }

    /**
     * Calls the connector table service getTableNames method.
     * @param uri location
     * @param prefixSearch if false, the method looks for exact match for the uri
     * @return list of table names
     */
    public List<QualifiedName> getQualifiedNames(final String uri, final boolean prefixSearch) {
        final List<QualifiedName> result = Lists.newArrayList();

        connectorManager.getTableServices().forEach(service -> {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            try {
                final Map<String, List<QualifiedName>> names =
                    service.getTableNames(connectorRequestContext, Lists.newArrayList(uri), prefixSearch);
                final List<QualifiedName> qualifiedNames = names.values().stream().flatMap(Collection::stream)
                    .collect(Collectors.toList());
                result.addAll(qualifiedNames);
            } catch (final UnsupportedOperationException uoe) {
                log.debug("Table service doesn't support getting table names by URI. Skipping");
            }
        });
        return result;
    }

    /**
     * Calls the connector table service getTableNames method.
     * @param uris list of locations
     * @param prefixSearch if false, the method looks for exact match for the uri
     * @return list of table names
     */
    public Map<String, List<QualifiedName>> getQualifiedNames(final List<String> uris, final boolean prefixSearch) {
        final Map<String, List<QualifiedName>> result = Maps.newHashMap();

        connectorManager.getTableServices().forEach(service -> {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            try {
                final Map<String, List<QualifiedName>> names =
                    service.getTableNames(connectorRequestContext, uris, prefixSearch);
                names.forEach((uri, qNames) -> {
                    final List<QualifiedName> existingNames = result.get(uri);
                    if (existingNames == null) {
                        result.put(uri, qNames);
                    } else {
                        existingNames.addAll(qNames);
                    }
                });
            } catch (final UnsupportedOperationException uoe) {
                log.debug("Table service doesn't support getting table names by URI. Skipping");
            }
        });
        return result;
    }

    /**
     * Calls the connector table service exists method.
     * @param name table name
     * @return true, if the object exists.
     */
    public boolean exists(final QualifiedName name) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(name);
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        return service.exists(connectorRequestContext, name);
    }

    /**
     * Returns a filtered list of table names.
     * @param name          catalog name
     * @param parameters    service parameters
     * @return list of table names
     */
    public List<QualifiedName> getQualifiedNames(final QualifiedName name,
                                                 final GetTableNamesServiceParameters parameters) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(name);
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        return service.getTableNames(connectorRequestContext, name, parameters.getFilter(), parameters.getLimit());
    }
}
