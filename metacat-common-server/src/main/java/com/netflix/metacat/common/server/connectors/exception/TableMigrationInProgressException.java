package com.netflix.metacat.common.server.connectors.exception;

import com.netflix.metacat.common.exception.MetacatTooManyRequestsException;

/**
 * Exception indicating that the table is currently under migration
 * and cannot be modified.
 *
 * Extends MetacatTooManyRequestsException so users can handle a HTTP 429 error code
 * to retry with a backoff until migration completes.
 */
public class TableMigrationInProgressException extends MetacatTooManyRequestsException {

    /**
     * Ctor.
     *
     * @param reason The exception message.
     */
    public TableMigrationInProgressException(final String reason) {
        super(reason);
    }
}
