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
package com.netflix.metacat.common.server.util;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.CatalogUnauthorizedException;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * Utility class for connector authorization checks.
 * Provides shared authorization logic for all connector service decorators.
 *
 * @author jursetta
 */
@Slf4j
public final class ConnectorAuthorizationUtil {

    private ConnectorAuthorizationUtil() {
        // Utility class - prevent instantiation
    }

    /**
     * Checks if the current caller is authorized to access the specified catalog.
     * Authorization is based on the userName from the request context.
     *
     * @param catalogName    the name of the catalog being accessed
     * @param allowedCallers the set of callers allowed to access this catalog
     * @param operation      the operation being performed (for logging)
     * @param resource       the resource being accessed (for logging)
     * @throws CatalogUnauthorizedException if the caller is not authorized
     */
    public static void checkAuthorization(
        final String catalogName,
        final Set<String> allowedCallers,
        final String operation,
        final QualifiedName resource
    ) {
        final MetacatRequestContext context = MetacatContextManager.getContext();
        final String caller = context.getUserName();

        if (caller == null) {
            log.warn("Authorization denied for catalog {}: No userName in request context for {} on {}",
                catalogName, operation, resource);
            throw new CatalogUnauthorizedException(catalogName);
        }

        if (!allowedCallers.contains(caller)) {
            log.warn("Authorization denied for catalog {}: Caller '{}' not in allowed list for {} on {}",
                catalogName, caller, operation, resource);
            throw new CatalogUnauthorizedException(catalogName, caller);
        }

        log.debug("Authorization granted for catalog {}: Caller '{}' for {} on {}",
            catalogName, caller, operation, resource);
    }
}
