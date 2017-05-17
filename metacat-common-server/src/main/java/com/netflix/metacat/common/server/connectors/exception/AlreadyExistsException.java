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
package com.netflix.metacat.common.server.connectors.exception;

import com.netflix.metacat.common.QualifiedName;
import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract not found error exception class.
 *
 * @author zhenl
 */
@Getter
public abstract class AlreadyExistsException extends ConnectorException {
    private QualifiedName name;

    protected AlreadyExistsException(@Nonnull @NonNull final QualifiedName name) {
        this(name, null);
    }

    protected AlreadyExistsException(@Nonnull @NonNull final QualifiedName name, @Nullable final Throwable cause) {
        this(name, cause, false, false);
    }

    protected AlreadyExistsException(
        final QualifiedName name,
        final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace
    ) {
        this(name, String.format("%s '%s' already exists.", name.getType().name(), name.toString()),
            cause, enableSuppression, writableStackTrace);
    }

    protected AlreadyExistsException(
        final QualifiedName name,
        final String message,
        final Throwable cause,
        final boolean enableSuppression,
        final boolean writableStackTrace
    ) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.name = name;
    }
}
