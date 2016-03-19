package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;

public class MetacatDeleteTablePreEvent extends MetacatEvent {

    public MetacatDeleteTablePreEvent(QualifiedName name, MetacatContext metacatContext) {
        super( name, metacatContext);
    }
}
