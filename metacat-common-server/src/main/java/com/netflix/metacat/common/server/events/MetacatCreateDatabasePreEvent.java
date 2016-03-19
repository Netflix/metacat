package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;

public class MetacatCreateDatabasePreEvent extends MetacatEvent {
    public MetacatCreateDatabasePreEvent(QualifiedName name, MetacatContext metacatContext) {
        super(name, metacatContext);
    }
}
