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

package com.netflix.metacat.common.server.exception;

import com.netflix.metacat.common.QualifiedName;

/**
 * Exception when a catalog is not found.
 * @author zhenl
 */
public class CatalogNotFoundException extends NotFoundException {

    /**
     * Constructor.
     * @param catalogName catalog name
     */
    public CatalogNotFoundException(final String catalogName) {
        this(catalogName, null);
    }

    /**
     * Constructor.
     * @param catalogName catalog name
     * @param cause error cause
     */
    public CatalogNotFoundException(final String catalogName, final Throwable cause) {
        super(QualifiedName.ofCatalog(catalogName), cause);
    }
}
