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
package com.netflix.metacat.common.json;

import lombok.Getter;

/**
 * Metacat JSON utility related exception.
 */
@Getter
public class MetacatJsonException extends RuntimeException {
    /**
     * Input json string if any.
     */
    private String inputJson;

    /**
     * Constructor.
     *
     * @param message exception message
     */
    public MetacatJsonException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param inputJson input json string
     * @param message details of the message
     * @param cause   exception cause
     */
    public MetacatJsonException(final String inputJson, final String message, final Throwable cause) {
        super(message, cause);
        this.inputJson = inputJson;
    }

    /**
     * Constructor.
     *
     * @param cause exception cause
     */
    public MetacatJsonException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message exception message
     * @param cause   exception cause
     */
    public MetacatJsonException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Default constructor.
     */
    public MetacatJsonException() {
        super();
    }
}
