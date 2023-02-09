package com.netflix.metacat.common.server.api.traffic_control;

import com.netflix.metacat.common.QualifiedName;
import lombok.NonNull;

/**
 * A default no-op gateway.
 */
public class DefaultRequestGateway implements RequestGateway {
    @Override
    public void validateRequest(@NonNull final String requestName, @NonNull final QualifiedName resource) {
        // no-op
    }
}
