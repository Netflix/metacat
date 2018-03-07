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

/**
 * Factory that returns the connector implementations of the service and converter interfaces.
 *
 * @author amajumdar
 * @since 1.0.0
 */
public interface ConnectorFactory {
    /**
     * Standard error message for all default implementations.
     */
    String UNSUPPORTED_MESSAGE = "Not supported by this connector";

    /**
     * Returns the database service implementation of the connector.
     *
     * @return Returns the database service implementation of the connector.
     */
    default ConnectorDatabaseService getDatabaseService() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table service implementation of the connector.
     *
     * @return Returns the table service implementation of the connector.
     */
    default ConnectorTableService getTableService() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the partition service implementation of the connector.
     *
     * @return Returns the partition service implementation of the connector.
     */
    default ConnectorPartitionService getPartitionService() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the name of the catalog.
     *
     * @return Returns the name of the catalog.
     */
    String getCatalogName();

    /**
     * Returns the name of the catalog shard.
     *
     * @return Returns the name of the catalog shard.
     */
    String getCatalogShardName();

    /**
     * Shuts down the factory.
     */
    void stop();
}
