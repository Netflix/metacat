package com.netflix.metacat.common.server.api.traffic_control;

import com.netflix.metacat.common.QualifiedName;
import lombok.NonNull;

/**
 * An interface to gate incoming requests against unauthorized operations,
 * blocked resources/apis etc.
 */
public interface RequestGateway {
    /**
     * Validate whether the request is alloed or not for the given resource. Implementations
     * are expected to throw the relevant exception with the right context and detail.
     *
     * @param requestName the name of the request (ex: getTable).
     * @param resource the primary resource of the request; like a table.
     */
    void validateRequest(@NonNull String requestName, @NonNull QualifiedName resource);
}
