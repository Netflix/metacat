/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.common.server.converter;

import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeRegistry;
import com.netflix.metacat.common.type.TypeSignature;
import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * Default type converter. Converter for metacat type representations.
 *
 * @author amajumdar
 * @since 1.0.0
 */
public class DefaultTypeConverter implements ConnectorTypeConverter {
    /**
     * {@inheritDoc}
     */
    @Override
    public Type toMetacatType(@Nonnull @NonNull final String type) {
        final TypeSignature signature = TypeSignature.parseTypeSignature(type);
        return TypeRegistry.getTypeRegistry().getType(signature);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String fromMetacatType(@Nonnull @NonNull final Type type) {
        return type.getDisplayName();
    }
}
