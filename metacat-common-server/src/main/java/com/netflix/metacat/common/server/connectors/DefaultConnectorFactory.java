/*
 *
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
 *
 */
package com.netflix.metacat.common.server.connectors;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Common connector factory with repeatable functionality.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Slf4j
@Getter
public class DefaultConnectorFactory implements ConnectorFactory {

    private final String name;
    private final Injector injector;

    /**
     * Constructor.
     *
     * @param name          The catalog name
     * @param modules The catalog configuration
     */
    public DefaultConnectorFactory(
        @Nonnull @NonNull final String name,
        @Nonnull @NonNull final Iterable<? extends Module> modules
    ) {
        log.info("Creating connector factory for catalog {}", name);
        this.name = name;
        this.injector = Guice.createInjector(modules);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return this.getService(ConnectorDatabaseService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTableService getTableService() {
        return this.getService(ConnectorTableService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorPartitionService getPartitionService() {
        return this.getService(ConnectorPartitionService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
    }

    private <T extends ConnectorBaseService> T getService(@Nonnull @NonNull final Class<T> serviceClass) {
        final T service = this.injector.getInstance(serviceClass);
        if (service != null) {
            return service;
        } else {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }
    }
}
