package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;

import java.util.Objects;

public class MetacatEvent {
    private final QualifiedName name;
    private final MetacatContext metacatContext;

    public MetacatEvent(QualifiedName name, MetacatContext metacatContext) {
        this.name = name;
        this.metacatContext = metacatContext;
    }

    public QualifiedName getName() {
        return name;
    }

    public MetacatContext getMetacatContext() {
        return metacatContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatEvent)) return false;
        MetacatEvent that = (MetacatEvent) o;
        return Objects.equals(name, that.name) && Objects.equals(metacatContext, that.metacatContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, metacatContext);
    }

    @Override
    public String toString() {
        return "MetacatEvent{" + "name=" + name + ", metacatContext=" + metacatContext + '}';
    }
}
