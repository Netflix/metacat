/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.main.services;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;

/**
 * Database service.
 */
public interface DatabaseService extends MetacatService<DatabaseDto> {
    /**
     * Gets the database for the given name.
     * @param name name
     * @param includeUserMetadata if true, will also include the user metadata
     * @param includeTableNames   if true, include the list of table names
     * @return database info
     */
    DatabaseDto get(QualifiedName name, boolean includeUserMetadata, boolean includeTableNames);
}
