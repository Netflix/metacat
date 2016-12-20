package com.netflix.metacat.common.type;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

/**
 * Decimal type class.
 */
@EqualsAndHashCode(callSuper = true)
public final class DecimalType extends AbstractType implements ParametricType {
    /** Default decimal type.*/
    public static final DecimalType DECIMAL = createDecimalType();
    /**If scale is not specified, it defaults to 0 (no fractional digits).*/
    private static final int DEFAULT_SCALE = 0;
    /** If no precision is specified, it defaults to 10.*/
    private static final int DEFAULT_PRECISION = 10;
    @Getter private final int precision;
    @Getter private final int scale;

    private DecimalType(final int precision, final int scale) {
        super(new TypeSignature(Base.DECIMAL.getBaseTypeDisplayName(), Lists.newArrayList(),
            Lists.newArrayList((long) precision, (long) scale)));
        Preconditions.checkArgument(precision >= 0, "Invalid decimal precision " + precision);
        Preconditions.checkArgument(scale >= 0 && scale <= precision, "Invalid decimal scale " + scale);
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Constructor.
     * @param precision precision
     * @param scale scale
     * @return DecimalType
     */
    public static DecimalType createDecimalType(final int precision, final int scale) {
        return new DecimalType(precision, scale);
    }

    /**
     * Constructor.
     * @param precision precision
     * @return DecimalType
     */
    public static DecimalType createDecimalType(final int precision) {
        return createDecimalType(precision, DEFAULT_SCALE);
    }

    /**
     * Constructor.
     * @return DecimalType
     */
    public static DecimalType createDecimalType() {
        return createDecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    @Override
    public String getParametricTypeName() {
        return Base.DECIMAL.getBaseTypeDisplayName();
    }

    @Override
    public Type createType(final List<Type> types, final List<Object> literals) {
        switch (literals.size()) {
            case 0:
                return DecimalType.createDecimalType();
            case 1:
                try {
                    return DecimalType.createDecimalType(Integer.valueOf(String.valueOf(literals.get(0))));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Decimal precision must be a number");
                }
            case 2:
                try {
                    return DecimalType.createDecimalType(Integer.valueOf(String.valueOf(literals.get(0))),
                        Integer.valueOf(String.valueOf(literals.get(0))));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Decimal parameters must be a number");
                }
            default:
                throw new IllegalArgumentException("Expected 0, 1 or 2 parameters for DECIMAL type constructor.");
        }
    }
}
