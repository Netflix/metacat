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
 * Error code.
 */
public enum StandardErrorCode
    implements ErrorCodeSupplier {
    /**
     * User error.
     */
    USER_ERROR(0x0000_0000),

    /**
     * Not found.
     */
    NOT_FOUND(0x0000_0005),

    /**
     * already exists.
      */
     ALREADY_EXISTS(0x0000_000C),
    /**
     * not supported.
      */
    NOT_SUPPORTED(0x0000_000D),

    /**
     * internal error.
      */

    INTERNAL_ERROR(0x0001_0000),
    /**
     * insufficient resources.
      */
    INSUFFICIENT_RESOURCES(0x0002_0000),

    /**
     * external error.
     */
    EXTERNAL(0x0100_0000);

    private final ErrorCode errorCode;

    StandardErrorCode(final int code) {
        errorCode = new ErrorCode(code, name());
    }

    /**
     * {@inheritdoc}.
     *
     */
    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }

    /**
     * Map error code error type.
     * @param code code
     * @return type
     */
    public static ErrorType toErrorType(final int code) {
        if (code < INTERNAL_ERROR.toErrorCode().getCode()) {
            return ErrorType.USER_ERROR;
        }
        if (code < INSUFFICIENT_RESOURCES.toErrorCode().getCode()) {
            return ErrorType.INTERNAL_ERROR;
        }
        if (code < EXTERNAL.toErrorCode().getCode()) {
            return ErrorType.INSUFFICIENT_RESOURCES;
        }
        return ErrorType.EXTERNAL;
    }

    /**
     * error type.
     */
    public enum ErrorType {
        /**
         * user error.
         */
        USER_ERROR,
        /**
         * internal error.
         */
        INTERNAL_ERROR,
        /**
         * insufficent resources.
         */
        INSUFFICIENT_RESOURCES,
        /**
         * external error.
         */
        EXTERNAL
    }
}
