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
//    SYNTAX_ERROR(0x0000_0001),
//    ABANDONED_QUERY(0x0000_0002),
//    USER_CANCELED(0x0000_0003),
//    PERMISSION_DENIED(0x0000_0004),

    /**
     * Not found.
     */
    NOT_FOUND(0x0000_0005),

//    FUNCTION_NOT_FOUND(0x0000_0006),
//    INVALID_FUNCTION_ARGUMENT(0x0000_0007),
//    DIVISION_BY_ZERO(0x0000_0008),
//    INVALID_CAST_ARGUMENT(0x0000_0009),
//    OPERATOR_NOT_FOUND(0x0000_000A),
//    INVALID_VIEW(0x0000_000B),
    /**
     * already exists.
      */
     ALREADY_EXISTS(0x0000_000C),
    /**
     * not supported.
      */
    NOT_SUPPORTED(0x0000_000D),
//    INVALID_SESSION_PROPERTY(0x0000_000E),
//    INVALID_WINDOW_FRAME(0x0000_000F),
//    CONSTRAINT_VIOLATION(0x0000_0010),
//    TRANSACTION_CONFLICT(0x0000_0011),
//
    /**
     * internal error.
      */
    INTERNAL_ERROR(0x0001_0000),
//    TOO_MANY_REQUESTS_FAILED(0x0001_0001),
//    PAGE_TOO_LARGE(0x0001_0002),
//    PAGE_TRANSPORT_ERROR(0x0001_0003),
//    PAGE_TRANSPORT_TIMEOUT(0x0001_0004),
//    NO_NODES_AVAILABLE(0x0001_0005),
//    REMOTE_TASK_ERROR(0x0001_0006),
//    COMPILER_ERROR(0x0001_0007),
//    WORKER_RESTARTED(0x0001_0008),
//    SERVER_SHUTTING_DOWN(0x0001_0009),
//
    /**
     * insufficient resources.
      */
    INSUFFICIENT_RESOURCES(0x0002_0000),
//    EXCEEDED_MEMORY_LIMIT(0x0002_0001),
//    QUERY_QUEUE_FULL(0x0002_0002),
//    EXCEEDED_TIME_LIMIT(0x0002_0003),
//    CLUSTER_OUT_OF_MEMORY(0x0002_0004),
//
//    // Connectors can use error codes starting at EXTERNAL
    /**
     * external error.
     */
    EXTERNAL(0x0100_0000);

    private final ErrorCode errorCode;

    StandardErrorCode(final int code) {
        errorCode = new ErrorCode(code, name());
    }

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
