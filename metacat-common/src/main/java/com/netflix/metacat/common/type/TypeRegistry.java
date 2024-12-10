/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.common.type;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Type mapping between canonical and connector types.
 *
 * @author zhenl
 */
public final class TypeRegistry implements TypeManager {
    //initailzed during class loading
    private static final TypeRegistry INSTANCE = new TypeRegistry();

    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<TypeEnum, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    private TypeRegistry() {
        Objects.requireNonNull(types, "types is null");
        addType(BaseType.UNKNOWN);
        addType(BaseType.BIGINT);
        addType(BaseType.BOOLEAN);
        addType(BaseType.FLOAT);
        addType(BaseType.DOUBLE);
        addType(BaseType.DATE);
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
        addParametricType(CharType.CHAR);
        addParametricType(MapType.MAP);
        addParametricType(RowType.ROW);
        addParametricType(ArrayType.ARRAY);
        addParametricType(VarbinaryType.VARBINARY);
        addParametricType(VarcharType.VARCHAR);
    }

    @SuppressFBWarnings(value = "MS_EXPOSE_REP", justification = "disable errors from MS_EXPOSE_REP ")
    public static TypeRegistry getTypeRegistry() {
        return INSTANCE;
    }

    /**
     * Verify type class isn't null.
     *
     * @param type parameter
     */
    public static void verifyTypeClass(final Type type) {
        Objects.requireNonNull(type, "types is null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type getType(final TypeSignature signature) {
        final Type type = types.get(signature);
        if (type == null) {
            return instantiateParametricType(signature);
        }
        return type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type getParameterizedType(final TypeEnum baseType,
                                     final List<TypeSignature> typeParameters,
                                     final List<Object> literalParameters) {
        return getType(new TypeSignature(baseType, typeParameters, literalParameters));
    }

    private Type instantiateParametricType(final TypeSignature signature) {
        final ImmutableList.Builder<Type> parameterTypes = ImmutableList.builder();
        for (TypeSignature parameter : signature.getParameters()) {
            final Type parameterType = getType(parameter);
            if (parameterType == null) {
                return null;
            }
            parameterTypes.add(parameterType);
        }

        final ParametricType parametricType = parametricTypes.get(signature.getBase());
        if (parametricType == null) {
            return null;
        }
        final Type instantiatedType = parametricType.createType(parameterTypes.build(),
            signature.getLiteralParameters());
        if (!instantiatedType.getTypeSignature().equals(signature)) {
            throw new IllegalStateException(String.format(
                "Instantiated parametric type name (%s) does not match expected name (%s)",
                instantiatedType, signature));
        }
        return instantiatedType;
    }

    /**
     * Add valid type to registry.
     *
     * @param type type
     */
    public void addType(final Type type) {
        verifyTypeClass(type);
        final Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        if (!(existingType == null || existingType.equals(type))) {
            throw new IllegalStateException(String.format("Type %s is already registered", type));
        }
    }

    /**
     * Add complex type to regiestry.
     *
     * @param parametricType Type
     */
    public void addParametricType(final ParametricType parametricType) {
        final TypeEnum baseType = parametricType.getBaseType();
        if (parametricTypes.containsKey(baseType)) {
            throw new IllegalArgumentException(String.format("Parametric type already registered: %s", baseType));
        }
        parametricTypes.putIfAbsent(baseType, parametricType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Type> getTypes() {
        return null;
    }
}
