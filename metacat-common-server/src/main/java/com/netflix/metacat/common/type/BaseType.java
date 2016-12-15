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

import java.util.Collections;
import java.util.List;

/**
 * Base class implements the type interface.
 */
public class BaseType implements Type {
    /**
     * BOOLEAN type.
     */
    public static final Type BOOLEAN = new BaseType(Base.BOOLEAN);
    /**
     * TINYINT type.
     */
    public static final Type TINYINT = new BaseType(Base.TINYINT);
    /**
     * SMALLINT type.
     */
    public static final Type SMALLINT = new BaseType(Base.SMALLINT);
    /**
     * INT type.
     */
    public static final Type INT = new BaseType(Base.INT);
    /**
     * BIGINT type.
     */
    public static final Type BIGINT = new BaseType(Base.BIGINT);
    /**
     * FLOAT type.
     */
    public static final Type FLOAT = new BaseType(Base.FLOAT);
    /**
     * DOUBLE type.
     */
    public static final Type DOUBLE = new BaseType(Base.DOUBLE);
    /**
     * STRING type.
     */
    public static final Type STRING = new BaseType(Base.STRING);
    /**
     * JSON type.
     */
    public static final Type JSON = new BaseType(Base.JSON);
    /**
     * VARBINARY type.
     */
    public static final Type VARBINARY = new BaseType(Base.VARBINARY);
    /**
     * DATE type.
     */
    public static final Type DATE = new BaseType(Base.DATE);
    /**
     * TIME type.
     */
    public static final Type TIME = new BaseType(Base.TIME);
    /**
     * TIME_WITH_TIME_ZONE type.
     */
    public static final Type TIME_WITH_TIME_ZONE = new BaseType(Base.TIME_WITH_TIME_ZONE);
    /**
     * TIMESTAMP type.
     */
    public static final Type TIMESTAMP = new BaseType(Base.TIMESTAMP);
    /**
     * TIMESTAMP_WITH_TIME_ZONE type.
     */
    public static final Type TIMESTAMP_WITH_TIME_ZONE = new BaseType(Base.TIMESTAMP_WITH_TIME_ZONE);
    /**
     * INTERVAL_YEAR_TO_MONTH type.
     */
    public static final Type INTERVAL_YEAR_TO_MONTH = new BaseType(Base.INTERVAL_YEAR_TO_MONTH);
    /**
     * INTERVAL_DAY_TO_SECOND type.
     */
    public static final Type INTERVAL_DAY_TO_SECOND = new BaseType(Base.INTERVAL_DAY_TO_SECOND);

    private Base base;
    private String sourceType;
    private List<Object> parameters;

    /**
     * BaseType constructor.
     * @param base base type
     */
    public BaseType(final Base base) {
        this(base, null, null);
    }

    /**
     * BaseType constructor.
     * @param base base type
     * @param parameters paramerters
     * @param sourceType sourcetype
     */
    public BaseType(final Base base, final List<Object> parameters, final String sourceType) {
        this.base = base;
        this.parameters = parameters == null ? Collections.emptyList() : parameters;
        this.sourceType = sourceType;
    }

    /**BaseType constructor.
     * @param base base type
     * @param sourceType source type
     */
    public BaseType(final Base base, final String sourceType) {
        this(base, null, sourceType);
    }

    @Override
    public Base getBaseType() {
        return base;
    }

    @Override
    public String getSignature() {
        final StringBuilder result = new StringBuilder(base.getType());
        if (!parameters.isEmpty()) {
            result.append("(");
            boolean first = true;
            for (Object parameter : parameters) {
                if (!first) {
                    result.append(",");
                } else {
                    first = false;
                }
                if (parameter instanceof String) {
                    result.append("'").append(parameter).append("'");
                } else {
                    result.append(parameter.toString());
                }
            }
            result.append(")");
        }
        return result.toString();
    }

    @Override
    public String getSourceType() {
        return sourceType;
    }
}
