/*
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.metacat.common.server.connector.iceberg;

import com.netflix.metacat.common.QualifiedName;

/**
 * Iceberg Table Criteria.
 *
 * @author zhenl
 * @since 1.2.0
 */
public interface IcebergTableCriteria {
    /**
     * To control iceberg table operation in metacat. The criteria implementation throws exception if
     * the iceberg table doesn't satisfy the criteria, e.g. the manifest file doesn't exist, or too large.
     *
     * @param tableName     qualified table name
     * @param tableLocation table location.
     */
    default void checkCriteria(final QualifiedName tableName, final String tableLocation) {
    }
}
