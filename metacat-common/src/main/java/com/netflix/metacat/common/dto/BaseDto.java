/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
