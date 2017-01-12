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

package com.netflix.metacat.canonical.type.converters;

import com.netflix.metacat.canonical.types.Type;
import com.netflix.metacat.canonical.types.TypeManager;

/**
 * Canonical type converter class.
 */
public interface CanonicalTypeConverter {
    /**
     * Converts to canonical type.
     * @param type type
     * @param typeRegistry typeRegistry
     * @return canonical type
     */
    Type dataTypeToCanonicalType(String type, TypeManager typeRegistry);

    /**
     * Converts from canonical type.
     *
     * @param type type
     * @return connector type
     */
    String canonicalTypeToDataType(Type type);
}
