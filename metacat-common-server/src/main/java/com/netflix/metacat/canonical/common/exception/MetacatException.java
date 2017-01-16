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
 * Metacat exception class.
 */
public class MetacatException
    extends RuntimeException {
    private final ErrorCode errorCode;

    /**
     * Constructor.
     * @param errorCode error code
     * @param message message
     */
    public MetacatException(final ErrorCodeSupplier errorCode, final String message) {
        this(errorCode, message, null);
    }

    /**
     * Constructor.
     * @param errorCode error code
     * @param throwable throwable
     */
    public MetacatException(final ErrorCodeSupplier errorCode, final Throwable throwable) {
        this(errorCode, null, throwable);
    }

    /**
     * Constructor.
     * @param errorCodeSupplier error code supplier
     * @param message message
     * @param cause cause
     */
    public MetacatException(final ErrorCodeSupplier errorCodeSupplier, final String message, final Throwable cause) {
        super(message, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    /**
     * Constructor.
     * @param errorCodeSupplier error code supplier
     * @param message message
     * @param cause cause
     * @param enableSuppression eable suppression
     * @param writableStackTrace stacktrace
     */
    public MetacatException(final ErrorCodeSupplier errorCodeSupplier, final String message,
                            final Throwable cause, final boolean enableSuppression,
                            final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * {@inheritdoc}.
     *
     */
    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }
}
