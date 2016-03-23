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

public class MetacatRenameTablePreEvent extends MetacatEvent {
    private final QualifiedName newName;

    public MetacatRenameTablePreEvent(QualifiedName newName, QualifiedName oldName, MetacatContext metacatContext) {
        super(oldName, metacatContext);
        this.newName = newName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatRenameTablePreEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatRenameTablePreEvent that = (MetacatRenameTablePreEvent) o;
        return Objects.equals(newName, that.newName);
    }

    public QualifiedName getNewName() {

        return newName;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(newName);
    }

    @Override
    public String toString() {
        return "MetacatRenameTablePreEvent{" +
                "newName=" + newName +
                '}';
    }
}
