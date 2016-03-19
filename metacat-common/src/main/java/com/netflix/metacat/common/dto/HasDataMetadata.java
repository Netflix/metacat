package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nonnull;

/**
 * Marker interface for objects with data metadata
 */
public interface HasDataMetadata extends HasMetadata {
    ObjectNode getDataMetadata();

    void setDataMetadata(ObjectNode metadata);

    /**
     * @return The uri that points to the location of the external data
     * @throws IllegalStateException if this instance does not have external data
     */
    @Nonnull
    String getDataUri();

    /**
     * @return true if this particular instance points to external data
     */
    boolean isDataExternal();
}
