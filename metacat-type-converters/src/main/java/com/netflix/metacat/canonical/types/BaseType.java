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

package com.netflix.metacat.canonical.types;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Base class implements the type interface.
 * @author zhenl
 */
@EqualsAndHashCode(callSuper = true)
public class BaseType extends AbstractType {
    /**
     * BOOLEAN type.
     */
    public static final Type BOOLEAN = createBaseType(Base.BOOLEAN.getBaseTypeDisplayName());
    /**
     * TINYINT type.
     */
    public static final Type TINYINT = createBaseType(Base.TINYINT.getBaseTypeDisplayName());
    /**
     * SMALLINT type.
     */
    public static final Type SMALLINT = createBaseType(Base.SMALLINT.getBaseTypeDisplayName());
    /**
     * INT type.
     */
    public static final Type INT = createBaseType(Base.INT.getBaseTypeDisplayName());
    /**
     * BIGINT type.
     */
    public static final Type BIGINT = createBaseType(Base.BIGINT.getBaseTypeDisplayName());
    /**
     * FLOAT type.
     */
    public static final Type FLOAT = createBaseType(Base.FLOAT.getBaseTypeDisplayName());
    /**
     * DOUBLE type.
     */
    public static final Type DOUBLE = createBaseType(Base.DOUBLE.getBaseTypeDisplayName());
    /**
     * STRING type.
     */
    public static final Type STRING = createBaseType(Base.STRING.getBaseTypeDisplayName());
    /**
     * JSON type.
     */
    public static final Type JSON = createBaseType(Base.JSON.getBaseTypeDisplayName());
    /**
     * VARBINARY type.
     */
    public static final Type VARBINARY = createBaseType(Base.VARBINARY.getBaseTypeDisplayName());
    /**
     * DATE type.
     */
    public static final Type DATE = createBaseType(Base.DATE.getBaseTypeDisplayName());
    /**
     * TIME type.
     */
    public static final Type TIME = createBaseType(Base.TIME.getBaseTypeDisplayName());
    /**
     * TIME_WITH_TIME_ZONE type.
     */
    public static final Type TIME_WITH_TIME_ZONE = createBaseType(Base.TIME_WITH_TIME_ZONE.getBaseTypeDisplayName());
    /**
     * TIMESTAMP type.
     */
    public static final Type TIMESTAMP = createBaseType(Base.TIMESTAMP.getBaseTypeDisplayName());
    /**
     * TIMESTAMP_WITH_TIME_ZONE type.
     */
    public static final Type TIMESTAMP_WITH_TIME_ZONE =
        createBaseType(Base.TIMESTAMP_WITH_TIME_ZONE.getBaseTypeDisplayName());
    /**
     * INTERVAL_YEAR_TO_MONTH type.
     */
    public static final Type INTERVAL_YEAR_TO_MONTH =
        createBaseType(Base.INTERVAL_YEAR_TO_MONTH.getBaseTypeDisplayName());
    /**
     * INTERVAL_DAY_TO_SECOND type.
     */
    public static final Type INTERVAL_DAY_TO_SECOND =
        createBaseType(Base.INTERVAL_DAY_TO_SECOND.getBaseTypeDisplayName());
//    /**
//     * DECIMAL type.
//     */
//    public static final Type DECIMAL = new BaseType(Base.DECIMAL);


    /**
     * UNKNOWN.
     */
    public static final Type UNKNOWN = createBaseType(Base.UNKNOWN.getBaseTypeDisplayName());

    @Getter
    protected final List<Type> parameters;
    @Getter
    @Setter
    private String sourceType;

    /**
     * BaseType constructor.
     *
     * @param signature base type
     */
    public BaseType(final TypeSignature signature) {
        this(signature, null);
    }

    /**
     * BaseType constructor.
     *
     * @param signature  base type
     * @param sourceType sourcetype
     */
    public BaseType(final TypeSignature signature, final String sourceType) {
        super(signature, sourceType);
        this.parameters = null;
        this.sourceType = sourceType;
    }

    private static BaseType createBaseType(final String baseType) {
        return new BaseType(new TypeSignature(baseType));
    }
}
