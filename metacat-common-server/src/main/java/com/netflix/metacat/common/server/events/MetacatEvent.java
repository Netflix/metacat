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

package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;

import java.util.Objects;

public class MetacatEvent {
    private final QualifiedName name;
    private final MetacatContext metacatContext;

    public MetacatEvent(QualifiedName name, MetacatContext metacatContext) {
        this.name = name;
        this.metacatContext = metacatContext;
    }

    public QualifiedName getName() {
        return name;
    }

    public MetacatContext getMetacatContext() {
        return metacatContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatEvent)) return false;
        MetacatEvent that = (MetacatEvent) o;
        return Objects.equals(name, that.name) && Objects.equals(metacatContext, that.metacatContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, metacatContext);
    }

    @Override
    public String toString() {
        return "MetacatEvent{" + "name=" + name + ", metacatContext=" + metacatContext + '}';
    }
}
