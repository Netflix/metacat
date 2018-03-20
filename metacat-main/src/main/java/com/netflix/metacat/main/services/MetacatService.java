/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


package com.netflix.metacat.main.services;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.BaseDto;

/**
 * Base service interface for all entities like catalog, database, table, view and partition.
 *
 * @author amajumdar,zhenl
 *
 * @param <T> Resource entity type.
 */
public interface MetacatService<T extends BaseDto> {
    /**
     * Creates the object.
     *
     * @param name qualified name of the object
     * @param dto  object metadata
     * @return created object
     */
    T create(QualifiedName name, T dto);

    /**
     * Updates the object.
     *
     * @param name qualified name of the object
     * @param dto  object dto
     */
    void update(QualifiedName name, T dto);


    /**
     * Updates the object and return the updated object.
     *
     * @param name qualified name of the object
     * @param dto  object dto
     * @return updated object
     */
    T updateAndReturn(QualifiedName name, T dto);

    /**
     * Deletes the object. Returns the metadata of the object deleted.
     *
     * @param name qualified name of the object to be deleted
     */
    void delete(QualifiedName name);

    /**
     * Returns the object with the given name.
     *
     * @param name qualified name of the object
     * @return Returns the object with the given name
     */
    T get(QualifiedName name);

    /**
     * Returns true, if the object exists.
     *
     * @param name qualified name of the object
     * @return boolean
     */
    boolean exists(QualifiedName name);
}
