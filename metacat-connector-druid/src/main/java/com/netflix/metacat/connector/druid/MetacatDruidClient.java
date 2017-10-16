/*
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
 */

package com.netflix.metacat.connector.druid;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;


/**
 * Druid Client.
 *
 * @author zhenl
 * @since 1.2.0
 */
public interface MetacatDruidClient {
    /**
     * Standard error message for all default implementations.
     */
    String UNSUPPORTED_MESSAGE = "Not supported for metacat druid client";

    /**
     * Get all data sources.
     *
     * @return data source names
     */
    default List<String> getAllDataSources() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns all segment data of data source.
     *
     * @param dataSourceName dataSourceName
     * @return data source raw data
     */
    default ObjectNode getAllDataByName(final String dataSourceName) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the latest segment of data source.
     *
     * @param dataSourceName dataSourceName
     * @return data source raw data
     */
    default ObjectNode getLatestDataByName(final String dataSourceName) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
}
