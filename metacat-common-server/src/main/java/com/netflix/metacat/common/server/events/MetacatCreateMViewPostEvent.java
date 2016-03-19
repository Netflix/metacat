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
