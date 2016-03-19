package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;

import java.util.Objects;

public class MetacatUpdateTablePostEvent extends MetacatEvent {
    private TableDto dto;

    public MetacatUpdateTablePostEvent(TableDto dto, MetacatContext metacatContext) {
        super(dto!=null?dto.getName():null, metacatContext);
        this.dto = dto;
    }

    public MetacatUpdateTablePostEvent(QualifiedName name, MetacatContext metacatContext) {
        super(name, metacatContext);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatUpdateTablePostEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatUpdateTablePostEvent that = (MetacatUpdateTablePostEvent) o;
        return Objects.equals(dto, that.dto);
    }

    public TableDto getDto() {
        return dto;
    }

    public void setDto(TableDto dto) {
        this.dto = dto;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(dto);
    }

    @Override
    public String toString() {
        return "MetacatUpdateTablePostEvent{dto=" + dto + '}';
    }
}
