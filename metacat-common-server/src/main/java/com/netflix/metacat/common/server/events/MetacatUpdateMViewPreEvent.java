package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;

import java.util.Objects;

public class MetacatUpdateMViewPreEvent extends MetacatEvent {
    private final TableDto dto;

    public MetacatUpdateMViewPreEvent(QualifiedName name, TableDto dto, MetacatContext metacatContext) {
        super(name, metacatContext);
        this.dto = dto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatUpdateMViewPreEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatUpdateMViewPreEvent that = (MetacatUpdateMViewPreEvent) o;
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
        return "MetacatUpdateMViewPreEvent{" +
                "dto=" + dto +
                '}';
    }
}
