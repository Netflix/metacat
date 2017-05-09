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
package com.netflix.metacat.common.exception;

import javax.ws.rs.core.Response;

/**
 * Exception from user metadata service.
 * TODO: This should be replaced by a BadRequestException from JAX-RS 2.x once we support the newer JAX-RS version.
 */
public class MetacatUserMetadataException extends MetacatException {
    /**
     * Constructor.
     *
     * @param message exception message
     */
    public MetacatUserMetadataException(final String message) {
        super(message, Response.Status.SEE_OTHER, null);
    }
}
