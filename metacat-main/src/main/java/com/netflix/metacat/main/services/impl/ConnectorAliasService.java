/*
 *  Copyright 2026 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.main.services.impl;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorFactoryDecorator;
import com.netflix.metacat.common.server.connectors.SpringConnectorFactory;
import com.netflix.metacat.common.server.connectors.exception.CatalogNotFoundException;
import com.netflix.metacat.common.server.usermetadata.AliasService;
import com.netflix.metacat.main.manager.ConnectorManager;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

/**
 * Alias service that delegates to the connector backing the catalog in the name.
 *
 * Catalogs whose connector registers no {@link AliasService} have no
 * aliases and their names pass through untouched.
 */
@RequiredArgsConstructor
public class ConnectorAliasService implements AliasService {
    private final ConnectorManager connectorManager;

    /**
     * {@inheritDoc}
     */
    @Override
    public QualifiedName getTableName(@NonNull final QualifiedName tableAlias) {
        return forCatalog(tableAlias)
            .map(aliasService -> aliasService.getTableName(tableAlias))
            .orElse(tableAlias);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAlias(@NonNull final QualifiedName name) {
        return forCatalog(name)
            .map(aliasService -> aliasService.isAlias(name))
            .orElse(false);
    }

    private Optional<AliasService> forCatalog(final QualifiedName name) {
        final ConnectorFactory factory;
        try {
            factory = connectorManager.getConnectorFactory(name);
        } catch (CatalogNotFoundException e) {
            // An unknown catalog is not this service's error to raise. Pass the name through so the
            // request fails where it normally would, with its usual message.
            return Optional.empty();
        }
        final ConnectorFactory delegate = factory instanceof ConnectorFactoryDecorator
            ? ((ConnectorFactoryDecorator) factory).getDelegate()
            : factory;
        return delegate instanceof SpringConnectorFactory
            ? Optional.ofNullable(((SpringConnectorFactory) delegate).getBeanIfAvailable(AliasService.class))
            : Optional.empty();
    }
}
