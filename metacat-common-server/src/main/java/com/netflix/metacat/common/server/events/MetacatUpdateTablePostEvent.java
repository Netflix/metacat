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
package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import javax.annotation.Nonnull;

/**
 * Post table update event.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatUpdateTablePostEvent extends MetacatEvent {
    private TableDto oldTable;
    private TableDto currentTable;
    private boolean isLatestCurrentTable;

    /**
     * Constructor.
     *
     * @param name           The name of the table that was updated
     * @param requestContext The metacat request context
     * @param source         The source object which threw this event
     * @param oldTable       The old DTO representation of the table
     * @param currentTable   The current DTO representation of the table
     */
    public MetacatUpdateTablePostEvent(
        @Nonnull @NonNull final QualifiedName name,
        @Nonnull @NonNull final MetacatRequestContext requestContext,
        @Nonnull @NonNull final Object source,
        @Nonnull @NonNull final TableDto oldTable,
        @Nonnull @NonNull final TableDto currentTable

    ) {
        this(name, requestContext, source, oldTable, currentTable, true);
    }

    /**
     * Constructor.
     *
     * @param name                  The name of the table that was updated
     * @param requestContext        The metacat request context
     * @param source                The source object which threw this event
     * @param oldTable              The old DTO representation of the table
     * @param currentTable          The current DTO representation of the table
     * @param isLatestCurrentTable  Whether the current table was successfully refreshed post-update
     */
    public MetacatUpdateTablePostEvent(
        @Nonnull @NonNull final QualifiedName name,
        @Nonnull @NonNull final MetacatRequestContext requestContext,
        @Nonnull @NonNull final Object source,
        @Nonnull @NonNull final TableDto oldTable,
        @Nonnull @NonNull final TableDto currentTable,
        final boolean isLatestCurrentTable

    ) {
        super(name, requestContext, source);
        this.oldTable = oldTable;
        this.currentTable = currentTable;
        this.isLatestCurrentTable = isLatestCurrentTable;
    }
}
