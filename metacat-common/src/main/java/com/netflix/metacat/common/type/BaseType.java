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

package com.netflix.metacat.common.type;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * TypeEnum class implements the type interface.
 * @author zhenl
 */
@EqualsAndHashCode(callSuper = true)
public class BaseType extends AbstractType {
    /**
     * BOOLEAN type.
     */
    public static final Type BOOLEAN = createBaseType(TypeEnum.BOOLEAN.getBaseTypeDisplayName());
    /**
     * TINYINT type.
     */
    public static final Type TINYINT = createBaseType(TypeEnum.TINYINT.getBaseTypeDisplayName());
    /**
     * SMALLINT type.
     */
    public static final Type SMALLINT = createBaseType(TypeEnum.SMALLINT.getBaseTypeDisplayName());
    /**
     * INT type.
     */
    public static final Type INT = createBaseType(TypeEnum.INT.getBaseTypeDisplayName());
    /**
     * BIGINT type.
     */
    public static final Type BIGINT = createBaseType(TypeEnum.BIGINT.getBaseTypeDisplayName());
    /**
     * FLOAT type.
     */
    public static final Type FLOAT = createBaseType(TypeEnum.FLOAT.getBaseTypeDisplayName());
    /**
     * DOUBLE type.
     */
    public static final Type DOUBLE = createBaseType(TypeEnum.DOUBLE.getBaseTypeDisplayName());
    /**
     * STRING type.
     */
    public static final Type STRING = createBaseType(TypeEnum.STRING.getBaseTypeDisplayName());
    /**
     * JSON type.
     */
    public static final Type JSON = createBaseType(TypeEnum.JSON.getBaseTypeDisplayName());
    /**
     * VARBINARY type.
     */
    public static final Type VARBINARY = createBaseType(TypeEnum.VARBINARY.getBaseTypeDisplayName());
    /**
     * DATE type.
     */
    public static final Type DATE = createBaseType(TypeEnum.DATE.getBaseTypeDisplayName());
    /**
     * TIME type.
     */
    public static final Type TIME = createBaseType(TypeEnum.TIME.getBaseTypeDisplayName());
    /**
     * TIME_WITH_TIME_ZONE type.
     */
    public static final Type TIME_WITH_TIME_ZONE =
        createBaseType(TypeEnum.TIME_WITH_TIME_ZONE.getBaseTypeDisplayName());
    /**
     * TIMESTAMP type.
     */
    public static final Type TIMESTAMP = createBaseType(TypeEnum.TIMESTAMP.getBaseTypeDisplayName());
    /**
     * TIMESTAMP_WITH_TIME_ZONE type.
     */
    public static final Type TIMESTAMP_WITH_TIME_ZONE =
        createBaseType(TypeEnum.TIMESTAMP_WITH_TIME_ZONE.getBaseTypeDisplayName());
    /**
     * INTERVAL_YEAR_TO_MONTH type.
     */
    public static final Type INTERVAL_YEAR_TO_MONTH =
        createBaseType(TypeEnum.INTERVAL_YEAR_TO_MONTH.getBaseTypeDisplayName());
    /**
     * INTERVAL_DAY_TO_SECOND type.
     */
    public static final Type INTERVAL_DAY_TO_SECOND =
        createBaseType(TypeEnum.INTERVAL_DAY_TO_SECOND.getBaseTypeDisplayName());
//    /**
//     * DECIMAL type.
//     */
//    public static final Type DECIMAL = new BaseType(TypeEnum.DECIMAL);


    /**
     * UNKNOWN.
     */
    public static final Type UNKNOWN = createBaseType(TypeEnum.UNKNOWN.getBaseTypeDisplayName());

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
