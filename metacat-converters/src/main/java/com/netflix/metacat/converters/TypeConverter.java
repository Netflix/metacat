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

package com.netflix.metacat.converters;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

/**
 * Type converter interface.
 */
public interface TypeConverter {
    /**
     * Converts to presto type.
     * @param type type
     * @param typeManager manager
     * @return presto type
     */
    Type toType(String type, TypeManager typeManager);

    /**
     * Converts from presto type.
     * @param type type
     * @return type
     */
    String fromType(Type type);
}
