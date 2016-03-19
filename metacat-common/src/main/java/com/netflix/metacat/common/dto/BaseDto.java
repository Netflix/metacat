package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.json.MetacatJsonLocator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public abstract class BaseDto implements Serializable {
    @Nullable
    public static ObjectNode deserializeObjectNode(@Nonnull ObjectInputStream inputStream) throws IOException {
        return MetacatJsonLocator.INSTANCE.deserializeObjectNode(inputStream);
    }

    public static void serializeObjectNode(@Nonnull ObjectOutputStream outputStream, @Nullable ObjectNode json)
            throws IOException {
        MetacatJsonLocator.INSTANCE.serializeObjectNode(outputStream, json);
    }

    @Override
    public String toString() {
        return MetacatJsonLocator.INSTANCE.toJsonString(this);
    }
}
