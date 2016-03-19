package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.dto.TableDto;

import java.util.Objects;

public class MetacatDeleteMViewPostEvent extends MetacatEvent {
    private final TableDto dto;

    public MetacatDeleteMViewPostEvent(TableDto dto, MetacatContext metacatContext) {
        super( dto!=null?dto.getName():null, metacatContext);
        this.dto = dto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatDeleteMViewPostEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatDeleteMViewPostEvent that = (MetacatDeleteMViewPostEvent) o;
        return Objects.equals(dto, that.dto);
    }

    public TableDto getDto() {
        return dto;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(dto);
    }

    @Override
    public String toString() {
        return "MetacatDeleteMViewPostEvent{dto=" + dto + '}';
    }
}
