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
import com.netflix.metacat.common.dto.TableDto;

import java.util.Objects;

public class MetacatCreateMViewPostEvent extends MetacatEvent {
    private final TableDto dto;
    private final String filter;
    private final Boolean snapshot;

    public MetacatCreateMViewPostEvent(TableDto dto, Boolean snapshot, String filter, MetacatContext metacatContext) {
        super( dto!=null?dto.getName():null, metacatContext);
        this.dto = dto;
        this.snapshot = snapshot;
        this.filter = filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatCreateMViewPostEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatCreateMViewPostEvent that = (MetacatCreateMViewPostEvent) o;
        return Objects.equals(dto, that.dto) &&
                Objects.equals(snapshot, that.snapshot) &&
                Objects.equals(filter, that.filter);
    }

    public TableDto getDto() {
        return dto;
    }

    public String getFilter() {
        return filter;
    }

    public Boolean getSnapshot() {
        return snapshot;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(dto, snapshot, filter);
    }

    @Override
    public String toString() {
        return "MetacatCreateMViewPostEvent{" +
                "dto=" + dto +
                ", snapshot=" + snapshot +
                ", filter='" + filter + '\'' +
                '}';
    }
}
