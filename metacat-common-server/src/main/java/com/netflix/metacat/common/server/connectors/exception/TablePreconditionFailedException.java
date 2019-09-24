/*
 *
 *  Copyright 2018 Netflix, Inc.
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
import lombok.Getter;

import javax.annotation.Nullable;

/**
 * Exception when table is not found.
 *
 * @author amajumdar
 */
@Getter
public class TablePreconditionFailedException extends ConnectorException {
    // Current metadata location for the table
    private final String metadataLocation;
    // Provided metadata location
    private final String previousMetadataLocation;
    /**
     * Constructor.
     *
     * @param name                      qualified name of the table
     * @param message                   error cause
     * @param metadataLocation          current metadata location
     * @param previousMetadataLocation  previous metadata location
     */
    public TablePreconditionFailedException(final QualifiedName name,
                                            @Nullable final String message,
                                            final String metadataLocation,
                                            final String previousMetadataLocation) {
        super(String.format("Precondition failed to update table %s. %s", name, message));
        this.metadataLocation = metadataLocation;
        this.previousMetadataLocation = previousMetadataLocation;
    }
}
