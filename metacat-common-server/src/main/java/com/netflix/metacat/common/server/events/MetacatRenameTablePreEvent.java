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
