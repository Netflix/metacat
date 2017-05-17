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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.springframework.context.ApplicationEvent;

import javax.annotation.Nonnull;

/**
 * Event within the Metacat JVM.
 *
 * @author amajumdar
 * @author tgianos
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatEvent extends ApplicationEvent {
    private final QualifiedName name;
    private final MetacatRequestContext requestContext;

    /**
     * Constructor.
     *
     * @param name           The qualified name of the resource this event pertains to
     * @param requestContext The request context that triggered this event
     * @param source         The source object this event was generated from
     */
    public MetacatEvent(
        @Nonnull @NonNull final QualifiedName name,
        @Nonnull @NonNull final MetacatRequestContext requestContext,
        @Nonnull @NonNull final Object source
    ) {
        super(source);
        this.name = name;
        this.requestContext = requestContext;
    }
}
