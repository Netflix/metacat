package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;

import java.util.Objects;

public class MetacatCreateMViewPreEvent extends MetacatEvent {
    private final String filter;
    private final Boolean snapshot;

    public MetacatCreateMViewPreEvent(QualifiedName name, Boolean snapshot, String filter, MetacatContext metacatContext) {
        super( name, metacatContext);
        this.snapshot = snapshot;
        this.filter = filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatCreateMViewPreEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatCreateMViewPreEvent that = (MetacatCreateMViewPreEvent) o;
        return Objects.equals(snapshot, that.snapshot) &&
                Objects.equals(filter, that.filter);
    }

    public String getFilter() {
        return filter;
    }

    public Boolean getSnapshot() {
        return snapshot;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(snapshot, filter);
    }

    @Override
    public String toString() {
        return "MetacatCreateMViewPreEvent{" +
                ", snapshot=" + snapshot +
                ", filter='" + filter + '\'' +
                '}';
    }
}
