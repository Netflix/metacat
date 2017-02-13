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

import javax.annotation.Nonnull;

/**
 * Post create metacat view event.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatCreateMViewPostEvent extends MetacatEvent {
    private final TableDto table;
    private final String filter;
    private final Boolean snapshot;

    /**
     * Constructor.
     * @param name name
     * @param requestContext context
     * @param table table info
     * @param snapshot snapshot
     * @param filter filter
     */
    public MetacatCreateMViewPostEvent(
        @Nonnull final QualifiedName name,
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final TableDto table,
        final Boolean snapshot,
        final String filter
    ) {
        super(name, requestContext);
        this.table = table;
        this.snapshot = snapshot;
        this.filter = filter;
    }
}
