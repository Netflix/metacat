package com.netflix.metacat.canonical.type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;


/**
 * Created by zhenli on 12/21/16.
 */
public class ArrayType extends AbstractType implements ParametricType {
    /** default.*/
    public static final ArrayType ARRAY = new ArrayType(BaseType.UNKNOWN);
    @Getter private final Type elementType;
    /**
     * Constructor.
     * @param elementType elementtype
     */
    public ArrayType(final Type elementType) {
        super(TypeUtils.parameterizedTypeName("array", elementType.getTypeSignature()));
        this.elementType = Preconditions.checkNotNull(elementType, "elementType is null");
    }

    @Override
    public String getParametricTypeName() {
        return Base.ARRAY.getBaseTypeDisplayName();
    }

    @Override
    public Type createType(final List<Type> types, final List<Object> literals) {
        Preconditions.checkArgument(types.size() == 1, "Expected only one type, got %s", types);
        Preconditions.checkArgument(literals.isEmpty(), "Unexpected literals: %s", literals);
        return new ArrayType(types.get(0));
    }

    @Override
    public List<Type> getParameters() {
        return ImmutableList.of(getElementType());
    }
}
