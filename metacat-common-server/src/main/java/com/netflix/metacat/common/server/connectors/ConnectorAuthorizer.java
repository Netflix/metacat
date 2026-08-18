/*
 *
 *  Copyright 2024 Netflix, Inc.
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

import com.netflix.metacat.common.QualifiedName;

/**
 * Service Provider Interface for connector-level authorization.
 *
 * @author abozigian
 */
public interface ConnectorAuthorizer {

    /**
     * Determines whether the current caller is authorized to perform the given operation
     * against the given catalog.
     *
     * @param catalogName       the name of the catalog being accessed
     * @param authorizedCallers the raw, deployment-defined {@code connector.authorized-callers}
     *                          configuration value for the catalog (may be empty)
     * @param operation         the operation being performed (for logging/decisioning)
     * @param resource          the resource being accessed (for logging/decisioning)
     * @throws com.netflix.metacat.common.server.connectors.exception.CatalogUnauthorizedException
     *         if the caller is not authorized
     */
    void checkAuthorization(String catalogName,
                            String authorizedCallers,
                            String operation,
                            QualifiedName resource);
}
