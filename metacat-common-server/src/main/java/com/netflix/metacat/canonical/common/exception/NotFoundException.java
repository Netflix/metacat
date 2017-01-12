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

package com.netflix.metacat.canonical.common.exception;

/**
 * Abstruct not found error exception class.
 */
public abstract class NotFoundException
    extends MetacatException {
    protected NotFoundException() {
        this(null, null);
    }

    protected NotFoundException(final String message) {
        this(message, null);
    }

    protected NotFoundException(final Throwable cause) {
        this(null, cause);
    }

    protected NotFoundException(final String message, final Throwable cause) {
        super(StandardErrorCode.NOT_FOUND, message, cause);
    }

    protected NotFoundException(final String message, final Throwable cause,
                                final boolean enableSuppression, final boolean writableStackTrace) {
        super(StandardErrorCode.NOT_FOUND, message, cause, enableSuppression, writableStackTrace);
    }
}
