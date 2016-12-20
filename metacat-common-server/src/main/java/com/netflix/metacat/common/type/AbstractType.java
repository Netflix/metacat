package com.netflix.metacat.common.type;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * AbstractType.
 */
@EqualsAndHashCode
public abstract class AbstractType implements Type {
    @Getter protected final TypeSignature signature;
    @Getter
    @Setter
    protected String sourceType;

    protected AbstractType(final TypeSignature signature) {
        this.signature = signature;
    }

    protected AbstractType(final TypeSignature signature, final String sourceType) {
        this.signature = signature;
        this.sourceType = sourceType;
    }
    /**
     * getTypeSignature.
     * @return TypeSignature
     */
    public final TypeSignature getTypeSignature() {
        return signature;
    }

    /**
     * get display name.
     * @return name
     */
    public String getDisplayName() {
        return signature.toString();
    }
}
