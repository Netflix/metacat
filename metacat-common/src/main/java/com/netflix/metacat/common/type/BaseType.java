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

/**
 * TypeEnum class implements the type interface.
 * @author zhenl
 */
@EqualsAndHashCode(callSuper = true)
public class BaseType extends AbstractType {
    /**
     * BOOLEAN type.
     */
    public static final Type BOOLEAN = createBaseType(TypeEnum.BOOLEAN);
    /**
     * TINYINT type.
     */
    public static final Type TINYINT = createBaseType(TypeEnum.TINYINT);
    /**
     * SMALLINT type.
     */
    public static final Type SMALLINT = createBaseType(TypeEnum.SMALLINT);
    /**
     * INT type.
     */
    public static final Type INT = createBaseType(TypeEnum.INT);
    /**
     * BIGINT type.
     */
    public static final Type BIGINT = createBaseType(TypeEnum.BIGINT);
    /**
     * FLOAT type.
     */
    public static final Type FLOAT = createBaseType(TypeEnum.FLOAT);
    /**
     * DOUBLE type.
     */
    public static final Type DOUBLE = createBaseType(TypeEnum.DOUBLE);
    /**
     * STRING type.
     */
    public static final Type STRING = createBaseType(TypeEnum.STRING);
    /**
     * JSON type.
     */
    public static final Type JSON = createBaseType(TypeEnum.JSON);
    /**
     * DATE type.
     */
    public static final Type DATE = createBaseType(TypeEnum.DATE);
    /**
     * TIME type.
     */
    public static final Type TIME = createBaseType(TypeEnum.TIME);
    /**
     * TIME_WITH_TIME_ZONE type.
     */
    public static final Type TIME_WITH_TIME_ZONE =
        createBaseType(TypeEnum.TIME_WITH_TIME_ZONE);
    /**
     * TIMESTAMP type.
     */
    public static final Type TIMESTAMP = createBaseType(TypeEnum.TIMESTAMP);
    /**
     * TIMESTAMP_WITH_TIME_ZONE type.
     */
    public static final Type TIMESTAMP_WITH_TIME_ZONE =
        createBaseType(TypeEnum.TIMESTAMP_WITH_TIME_ZONE);
    /**
     * INTERVAL_YEAR_TO_MONTH type.
     */
    public static final Type INTERVAL_YEAR_TO_MONTH =
        createBaseType(TypeEnum.INTERVAL_YEAR_TO_MONTH);
    /**
     * INTERVAL_DAY_TO_SECOND type.
     */
    public static final Type INTERVAL_DAY_TO_SECOND =
        createBaseType(TypeEnum.INTERVAL_DAY_TO_SECOND);


    /**
     * UNKNOWN.
     */
    public static final Type UNKNOWN = createBaseType(TypeEnum.UNKNOWN);

    /**
     * BaseType constructor.
     *
     * @param signature base type
     */
    public BaseType(final TypeSignature signature) {
        super(signature);
    }

    private static BaseType createBaseType(final TypeEnum baseType) {
        return new BaseType(new TypeSignature(baseType));
    }
}
