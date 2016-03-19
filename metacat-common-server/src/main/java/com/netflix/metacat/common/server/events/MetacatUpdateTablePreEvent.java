package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;

import java.util.Objects;

public class MetacatUpdateTablePreEvent extends MetacatEvent {
    private final TableDto table;

    public MetacatUpdateTablePreEvent(QualifiedName name, TableDto table, MetacatContext metacatContext) {
        super(name, metacatContext);
        this.table = table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatUpdateTablePreEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatUpdateTablePreEvent that = (MetacatUpdateTablePreEvent) o;
        return Objects.equals(table, that.table);
    }

    public TableDto getTable() {

        return table;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash( table);
    }

    @Override
    public String toString() {
        return "MetacatUpdateTablePreEvent{" +
                ", table=" + table +
                '}';
    }
}
