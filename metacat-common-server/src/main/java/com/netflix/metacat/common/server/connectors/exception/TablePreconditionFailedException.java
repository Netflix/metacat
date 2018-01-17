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

import javax.annotation.Nullable;

/**
 * Exception when table is not found.
 *
 * @author amajumdar
 */
public class TablePreconditionFailedException extends ConnectorException {
    /**
     * Constructor.
     *
     * @param name  qualified name of the table
     * @param message error cause
     */
    public TablePreconditionFailedException(final QualifiedName name, @Nullable final String message) {
        super(String.format("Precondition failed to update table %s. %s", name, message));
    }
}
