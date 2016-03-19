package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;

public class MetacatUpdateDatabasePostEvent extends MetacatEvent {
    public MetacatUpdateDatabasePostEvent(QualifiedName name,
            MetacatContext metacatContext) {
        super(name, metacatContext);
    }
}
