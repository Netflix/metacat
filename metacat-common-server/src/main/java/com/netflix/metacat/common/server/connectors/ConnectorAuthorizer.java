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
 * <p>When a catalog is configured with {@code connector.authorization-required=true}, the
 * {@link ConnectorFactoryDecorator} wraps every connector service with an authorization
 * decorator that delegates the access decision to an implementation of this interface.
 *
 * @author abozigian
 */
public interface ConnectorAuthorizer {

    /**
     * Determines whether the current caller is authorized to perform the given operation
     * against the given catalog.
     *
     * <p>The caller identity is not passed as an argument; implementations are expected to
     * resolve it from the ambient request context.
     *
     * @param catalogName       the name of the catalog being accessed
     * @param authorizedCallers the raw, deployment-defined {@code connector.authorized-callers}
     *                          configuration value for the catalog (may be empty)
     * @param operation         the operation being performed (for logging/decisioning)
     * @param resource          the resource being accessed (for logging/decisioning)
     * @return {@code true} if the caller is authorized, {@code false} otherwise
     */
    boolean isAuthorized(String catalogName,
                         String authorizedCallers,
                         String operation,
                         QualifiedName resource);
}
