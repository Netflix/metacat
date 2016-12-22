package com.netflix.metacat.canonical.type;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    @Override
    public List<Type> getParameters() {
        return Collections.unmodifiableList(new ArrayList<>());
    }
}
