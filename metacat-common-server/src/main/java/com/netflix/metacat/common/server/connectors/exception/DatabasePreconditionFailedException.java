package com.netflix.metacat.common.server.connectors.exception;

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

import com.netflix.metacat.common.QualifiedName;
import lombok.Getter;

import javax.annotation.Nullable;

/**
 * Exception when database can't be deleted because ON DELETE CASCADE is
 * disabled and a table still exists in the database.
 *
 * @author gtret
 */
@Getter
public class DatabasePreconditionFailedException extends ConnectorException {
    /**
     * Constructor.
     *
     * @param name                      qualified name of the database
     * @param message                   error description
     * @param error                     stacktrace
     */
    public DatabasePreconditionFailedException(final QualifiedName name,
                                               @Nullable final String message,
                                               @Nullable final Throwable error) {
        super(String.format("Precondition failed to update table %s. %s", name, message), error);
    }
}


