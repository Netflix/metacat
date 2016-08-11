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
import com.netflix.metacat.common.dto.TableDto;

import java.util.Objects;

public class MetacatUpdateTablePreEvent extends MetacatEvent {
    private final TableDto table;
    private final TableDto oldTable;

    public MetacatUpdateTablePreEvent(QualifiedName name, TableDto oldTable, TableDto table,
            MetacatContext metacatContext) {
        super(name, metacatContext);
        this.oldTable = oldTable;
        this.table = table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        MetacatUpdateTablePreEvent that = (MetacatUpdateTablePreEvent) o;
        return Objects.equals(table, that.table) && Objects.equals(oldTable, that.oldTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table, oldTable);
    }

    public TableDto getOldTable() {
        return oldTable;
    }

    public TableDto getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "MetacatUpdateTablePreEvent{" + "table=" + table + ", oldTable=" + oldTable + '}';
    }
}
