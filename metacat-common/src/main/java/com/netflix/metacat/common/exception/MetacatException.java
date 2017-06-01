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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonLocator;

/**
 * Base exception for Metacat errors exposed externally.
 *
 * @author amajumdar
 * @author tgianos
 */
public class MetacatException extends RuntimeException {
    private static final MetacatJson METACAT_JSON = MetacatJsonLocator.INSTANCE;
    private static final ObjectNode EMPTY_ERROR = METACAT_JSON.emptyObjectNode().put("error", "");

    /**
     * Constructor.
     */
    public MetacatException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param msg The error message to pass along
     */
    public MetacatException(final String msg) {
        super(msg);
    }

    /**
     * Constructor.
     *
     * @param msg   The error message to pass along
     * @param cause The cause of the error
     */
    public MetacatException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
