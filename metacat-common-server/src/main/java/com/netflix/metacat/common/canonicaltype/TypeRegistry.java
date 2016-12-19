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

package com.netflix.metacat.common.canonicaltype;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/**
 * Type mapping between canonical and connector types.
 */
public final class TypeRegistry implements TypeManager {

    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public TypeRegistry() {
        Preconditions.checkNotNull(types, "types is null");
        addType(BaseType.UNKNOWN);

        addType(BaseType.BIGINT);
        addType(BaseType.BOOLEAN);
        addType(BaseType.FLOAT);
        addType(BaseType.INT);
        addType(BaseType.SMALLINT);
        addType(BaseType.TINYINT);
        addType(BaseType.JSON);
        addType(BaseType.TIME);
        addType(BaseType.TIME_WITH_TIME_ZONE);
        addType(BaseType.INTERVAL_DAY_TO_SECOND);
        addType(BaseType.INTERVAL_YEAR_TO_MONTH);
        addType(BaseType.STRING);
        addType(BaseType.TIMESTAMP);
        addType(BaseType.TIMESTAMP_WITH_TIME_ZONE);

        addParametricType(DecimalType.DECIMAL);

    }

    @Override
    public Type getType(final TypeSignature signature) {
        final Type type = types.get(signature);
        if (type == null) {
            return instantiateParametricType(signature);
        }
        return type;
    }

    @Override
    public Type getParameterizedType(final String baseTypeName,
                                     final List<TypeSignature> typeParameters,
                                     final List<Object> literalParameters) {
        return getType(new TypeSignature(baseTypeName, typeParameters, literalParameters));
    }

    private Type instantiateParametricType(final TypeSignature signature) {
        ImmutableList.Builder<Type> parameterTypes = ImmutableList.builder();
        for (TypeSignature parameter : signature.getParameters()) {
            Type parameterType = getType(parameter);
            if (parameterType == null) {
                return null;
            }
            parameterTypes.add(parameterType);
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            return null;
        }
        Type instantiatedType = parametricType.createType(parameterTypes.build(), signature.getLiteralParameters());
        checkState(instantiatedType.getTypeSignature().equals(signature), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType, signature);
        return instantiatedType;
    }

    /**
     * Verify type class isn't null.
     * @param type parameter
     */
    public static void verifyTypeClass(final Type type) {
        Preconditions.checkNotNull(type, "type is null");
    }


    private static final Map<Type, String> CANONICALTOHIVE = ImmutableMap.<Type, String>builder()
        .put(BaseType.TINYINT, Base.TINYINT.getBaseTypeDisplayName())
        .put(BaseType.SMALLINT, Base.SMALLINT.getBaseTypeDisplayName())
        .put(BaseType.INT, Base.INT.getBaseTypeDisplayName())
        .put(BaseType.BIGINT, Base.BIGINT.getBaseTypeDisplayName())
        .put(BaseType.FLOAT, Base.FLOAT.getBaseTypeDisplayName())
        .put(BaseType.DOUBLE, Base.DOUBLE.getBaseTypeDisplayName())
        .put(BaseType.BOOLEAN, Base.BOOLEAN.getBaseTypeDisplayName())
//        .put(Base.DECIMAL, BaseType.DECIMAL)
//        .put(Base.CHAR, BaseType.CHAR)
//        .put(Base.VARCHAR, BaseType.VARCHAR)
        .put(BaseType.STRING, Base.STRING.getBaseTypeDisplayName())
        .put(BaseType.JSON, Base.STRING.getBaseTypeDisplayName())
        .put(BaseType.VARBINARY, Base.VARBINARY.getBaseTypeDisplayName())
        .put(BaseType.DATE, Base.DATE.getBaseTypeDisplayName())
        .put(BaseType.TIMESTAMP, Base.TIMESTAMP.getBaseTypeDisplayName())
        .build();

    private static final Map<String, Type> HIVETOCANONICAL = ImmutableMap.<String, Type>builder()
        .put(Base.BOOLEAN.getBaseTypeDisplayName(), BaseType.BOOLEAN)
        .put(Base.TINYINT.getBaseTypeDisplayName(), BaseType.TINYINT)
        .put(Base.SMALLINT.getBaseTypeDisplayName(), BaseType.SMALLINT)
        .put(Base.INT.getBaseTypeDisplayName(), BaseType.INT)
        .put(Base.BIGINT.getBaseTypeDisplayName(), BaseType.BIGINT)
        .put(Base.FLOAT.getBaseTypeDisplayName(), BaseType.FLOAT)
        .put(Base.DOUBLE.getBaseTypeDisplayName(), BaseType.DOUBLE)
//        .put(Base.DECIMAL, BaseType.DECIMAL)
//        .put(Base.CHAR, BaseType.CHAR)
//        .put(Base.VARCHAR, BaseType.VARCHAR)
        .put(Base.STRING.getBaseTypeDisplayName(), BaseType.STRING)
        .put("binary", BaseType.VARBINARY)
        .put(Base.DATE.getBaseTypeDisplayName(), BaseType.DATE)
        .put(Base.TIMESTAMP.getBaseTypeDisplayName(), BaseType.TIMESTAMP)
        .build();



    /**
     * Add valid type to registry.
     * @param type
     */
    public void addType(final Type type) {
        verifyTypeClass(type);
        final Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        Preconditions.checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    /**
     * Add complex type to regiestry.
     * @param parametricType Type
     */
    public void addParametricType(final ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }
    /**
     * Get canonical type to hive type mapping.
     * @return mapping from canonical type to hive type
     */
    public static Map<Type, String> getCanonicalToHiveType() {
        return CANONICALTOHIVE;
    }

    /**
     * Get hive type from to canonical type mapping.
     * @return mapping from hive to canonical type
     */
    public static Map<String, Type> getHiveToCanonical() {
        return HIVETOCANONICAL;
    }

    @Override
    public List<Type> getTypes() {
        return null;
    }
}
