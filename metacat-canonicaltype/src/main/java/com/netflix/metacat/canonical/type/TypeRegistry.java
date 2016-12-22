package com.netflix.metacat.canonical.type;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/**
 * Type mapping between canonical and connector types.
 */
public class TypeRegistry implements TypeManager {

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
        addType(BaseType.DOUBLE);
        addType(BaseType.DATE);
        addType(BaseType.VARBINARY);
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
        final ImmutableList.Builder<Type> parameterTypes = ImmutableList.builder();
        for (TypeSignature parameter : signature.getParameters()) {
            final Type parameterType = getType(parameter);
            if (parameterType == null) {
                return null;
            }
            parameterTypes.add(parameterType);
        }

        final ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            return null;
        }
        final Type instantiatedType = parametricType.createType(parameterTypes.build(), signature.getLiteralParameters());
        Preconditions.checkState(instantiatedType.getTypeSignature().equals(signature),
            "Instantiated parametric type name (%s) does not match expected name (%s)",
            instantiatedType, signature);
        return instantiatedType;
    }

    /**
     * Verify type class isn't null.
     * @param type parameter
     */
    public static void verifyTypeClass(final Type type) {
        Preconditions.checkNotNull(type, "type is null");
    }



    /**
     * Add valid type to registry.
     * @param type type
     */
    public void addType(final Type type) {
        verifyTypeClass(type);
        final Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    /**
     * Add complex type to regiestry.
     * @param parametricType Type
     */
    public void addParametricType(final ParametricType parametricType) {
        final String name = parametricType.getParametricTypeName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }


    @Override
    public List<Type> getTypes() {
        return null;
    }
}
