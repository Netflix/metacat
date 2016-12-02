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
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.CreateCatalogDto;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Catalog service.
 */
public interface CatalogService {
    /**
     * Gets the catalog.
     * @param name Qualified name of the catalog
     * @return the information about the given catalog
     */
    @Nonnull
    CatalogDto get(
        @Nonnull
            QualifiedName name);

    /**
     * List of registered catalogs.
     * @return all of the registered catalogs
     */
    @Nonnull
    List<CatalogMappingDto> getCatalogNames();

    /**
     * Updates the catalog.
     * @param name             Qualified name of the catalog
     * @param createCatalogDto catalog
     */
    @Nonnull
    void update(
        @Nonnull
            QualifiedName name,
        @Nonnull
            CreateCatalogDto createCatalogDto);
}
