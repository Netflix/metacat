package com.netflix.metacat.canonical.type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Created by zhenli on 12/20/16.
 */

@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
public class MapType extends AbstractType implements ParametricType {
    /**default.*/
    public static final MapType MAP = new MapType(BaseType.UNKNOWN, BaseType.UNKNOWN);

    private final Type keyType;
    private final Type valueType;

    /**
     * Constructor.
     * @param keyType keytype
     * @param valueType valuetype
     */
    public MapType(final Type keyType, final Type valueType) {
         super(TypeUtils.parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()));
         this.keyType = keyType;
         this.valueType = valueType;
    }

    @Override
    public String getDisplayName() {
         return "map<" + keyType.getTypeSignature().toString()
            + ", " + valueType.getTypeSignature().toString() + ">";
    }

    @Override
    public List<Type> getParameters() {
         return ImmutableList.of(getKeyType(), getValueType());
    }

    @Override
    public String getParametricTypeName() {
        return Base.MAP.getBaseTypeDisplayName();
    }

    @Override
    public Type createType(List<Type> types, List<Object> literals) {
        Preconditions.checkArgument(types.size() == 2, "Expected two types");
        Preconditions.checkArgument(literals.isEmpty(), "Unexpected literals: %s", literals);
        return new MapType(types.get(0), types.get(1));

    }

}
