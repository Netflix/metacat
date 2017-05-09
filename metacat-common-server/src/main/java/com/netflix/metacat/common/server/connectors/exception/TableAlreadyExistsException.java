/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.netflix.metacat.common.QualifiedName;

/**
 * Exception when schema already exists.
 *
 * @author zhenl
 */
public class TableAlreadyExistsException extends AlreadyExistsException {

    /**
     * Constructor.
     *
     * @param tableName table name
     */
    public TableAlreadyExistsException(final QualifiedName tableName) {
        this(tableName, null);
    }

    /**
     * Constructor.
     *
     * @param tableName table name
     * @param cause     error cause
     */
    public TableAlreadyExistsException(final QualifiedName tableName, final Throwable cause) {
        super(tableName, cause);
    }
}
