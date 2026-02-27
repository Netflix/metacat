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
package com.netflix.metacat.common.server.connectors.exception;

import javax.annotation.Nullable;

/**
 * Exception thrown when a caller is not authorized to access a catalog.
 *
 * @author jursetta
 */
public class CatalogUnauthorizedException extends ConnectorException {

    /**
     * Constructor.
     *
     * @param catalogName the catalog that was accessed
     * @param caller      the caller that attempted access
     */
    public CatalogUnauthorizedException(final String catalogName, @Nullable final String caller) {
        super(buildMessage(catalogName, caller));
    }

    /**
     * Constructor.
     *
     * @param catalogName the catalog that was accessed
     */
    public CatalogUnauthorizedException(final String catalogName) {
        super(String.format("Access to catalog '%s' requires an authenticated caller", catalogName));
    }

    private static String buildMessage(final String catalogName, @Nullable final String caller) {
        if (caller == null) {
            return String.format("Access to catalog '%s' requires an authenticated caller", catalogName);
        }
        return String.format("Caller '%s' is not authorized to access catalog '%s'", caller, catalogName);
    }
}
