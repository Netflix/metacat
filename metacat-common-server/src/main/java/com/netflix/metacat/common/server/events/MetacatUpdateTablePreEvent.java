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
import lombok.ToString;

import javax.validation.constraints.NotNull;

/**
 * Pre table update event.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatUpdateTablePreEvent extends MetacatEvent {

    private final TableDto currentTable;
    private final TableDto oldTable;

    /**
     * Constructor.
     * @param name name
     * @param requestContext context
     * @param oldTable old table info
     * @param currentTable new table info
     */
    public MetacatUpdateTablePreEvent(
        @NotNull
        final QualifiedName name,
        @NotNull
        final MetacatRequestContext requestContext,
        @NotNull
        final TableDto oldTable,
        @NotNull
        final TableDto currentTable
    ) {
        super(name, requestContext);
        this.oldTable = oldTable;
        this.currentTable = currentTable;
    }
}
