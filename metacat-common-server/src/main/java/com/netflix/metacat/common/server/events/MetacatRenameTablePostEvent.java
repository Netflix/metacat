package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;

import java.util.Objects;

public class MetacatRenameTablePostEvent extends MetacatEvent {
    private final TableDto dto;

    public MetacatRenameTablePostEvent(QualifiedName oldName, TableDto dto, MetacatContext metacatContext) {
        super(oldName, metacatContext);
        this.dto = dto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatRenameTablePostEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatRenameTablePostEvent that = (MetacatRenameTablePostEvent) o;
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
        return "MetacatRenameTablePostEvent{" +
                "dto=" + dto +
                '}';
    }
}
