package com.netflix.metacat.common.canonicaltype;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Created by zhenli on 12/18/16.
 */
public enum ParameterKind {
    /** . */
    TYPE("TYPE_SIGNATURE"),
    /** . */
    NAMED_TYPE("NAMED_TYPE_SIGNATURE"),
    /** . */
    LONG("LONG_LITERAL"),
    /** . */
    VARIABLE("");

    private final String name;
    ParameterKind(final String name) {
        this.name = name;
    }

    @JsonValue
    public String jsonName()
    {
        return name;
    }

    @JsonCreator
    public static ParameterKind fromJsonValue(final String value) {
        for (ParameterKind kind : values()) {
            if (kind.name().equals(value)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("Invalid serialized ParameterKind value: " + value);
    }
}
