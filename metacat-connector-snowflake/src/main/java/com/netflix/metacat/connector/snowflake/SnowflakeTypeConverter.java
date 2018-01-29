/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package com.netflix.metacat.connector.snowflake;

import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarcharType;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import lombok.extern.slf4j.Slf4j;

/**
 * Type converter for Snowflake.
 *
 * @author amajumdar
 * @since 1.2.0
 */
@Slf4j
public class SnowflakeTypeConverter extends JdbcTypeConverter {

    static final int DEFAULT_CHARACTER_LENGTH = 256;
    private static final String DEFAULT_CHARACTER_LENGTH_STRING = Integer.toString(DEFAULT_CHARACTER_LENGTH);

    /**
     * {@inheritDoc}
     *
     * @see <a href="https://docs.snowflake.net/manuals/sql-reference/data-types.html">Snowflake Types</a>
     */
    @Override
    public Type toMetacatType(final String type) {
        final String lowerType = type.toLowerCase();

        // Split up the possible type: TYPE[(size, magnitude)] EXTRA
        final String[] splitType = this.splitType(lowerType);
        switch (splitType[0]) {
            case "smallint":
            case "tinyint":
            case "byteint":
                return BaseType.SMALLINT;
            case "int":
            case "integer":
                return BaseType.INT;
            case "bigint":
                return BaseType.BIGINT;
            case "number":
            case "decimal":
            case "numeric":
                return this.toMetacatDecimalType(splitType);
            case "real":
            case "float4":
                return BaseType.FLOAT;
            case "double":
            case "double precision":
            case "float8":
            case "float":
                return BaseType.DOUBLE;
            case "varchar":
                fixDataSizeIfIncorrect(splitType);
                return this.toMetacatVarcharType(splitType);
            case "text":
            case "string":
                // text is basically alias for VARCHAR(256)
                splitType[1] = DEFAULT_CHARACTER_LENGTH_STRING;
                return this.toMetacatVarcharType(splitType);
            case "character":
            case "char":
                fixDataSizeIfIncorrect(splitType);
                return this.toMetacatCharType(splitType);
            case "binary":
            case "varbinary":
                fixDataSizeIfIncorrect(splitType);
                return this.toMetacatVarbinaryType(splitType);
            case "timestamp":
            case "datetime":
            case "timestamp_ntz":
            case "timestampntz":
            case "timestamp without time zone":
                return this.toMetacatTimestampType(splitType);
            case "timestamp_tz":
            case "timestamptz":
            case "timestampltz":
            case "timestamp_ltz":
            case "timestamp with local time zone":
            case "timestamp with time zone":
                return BaseType.TIMESTAMP_WITH_TIME_ZONE;
            case "date":
                return BaseType.DATE;
            case "boolean":
                return BaseType.BOOLEAN;
            default:
                log.info("Unhandled or unknown Snowflake type {}", splitType[0]);
                return BaseType.UNKNOWN;
        }
    }

    private void fixDataSizeIfIncorrect(final String[] splitType) {
        //
        // Adding a hack to ignore errors for data type with negative size.
        // TODO: Remove this hack when we have a solution for the above.
        //
        if (splitType[1] == null || Integer.parseInt(splitType[1]) <= 0) {
            splitType[1] = DEFAULT_CHARACTER_LENGTH_STRING;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String fromMetacatType(final Type type) {
        switch (type.getTypeSignature().getBase()) {
            case ARRAY:
                throw new UnsupportedOperationException("Snowflake doesn't support array types");
            case BIGINT:
                return "NUMBER(38)";
            case BOOLEAN:
                return "BOOLEAN";
            case CHAR:
                if (!(type instanceof CharType)) {
                    throw new IllegalArgumentException("Expected CHAR type but was " + type.getClass().getName());
                }
                final CharType charType = (CharType) type;
                return "CHAR(" + charType.getLength() + ")";
            case DATE:
                return "DATE";
            case DECIMAL:
                if (!(type instanceof DecimalType)) {
                    throw new IllegalArgumentException("Expected decimal type but was " + type.getClass().getName());
                }
                final DecimalType decimalType = (DecimalType) type;
                return "DECIMAL(" + decimalType.getPrecision() + ", " + decimalType.getScale() + ")";
            case DOUBLE:
            case FLOAT:
                return "DOUBLE PRECISION";
            case INT:
                return "INT";
            case INTERVAL_DAY_TO_SECOND:
                throw new UnsupportedOperationException("Snowflake doesn't support interval types");
            case INTERVAL_YEAR_TO_MONTH:
                throw new UnsupportedOperationException("Snowflake doesn't support interval types");
            case JSON:
                throw new UnsupportedOperationException("Snowflake doesn't support JSON types");
            case MAP:
                throw new UnsupportedOperationException("Snowflake doesn't support MAP types");
            case ROW:
                throw new UnsupportedOperationException("Snowflake doesn't support ROW types");
            case SMALLINT:
                return "SMALLINT";
            case STRING:
                return "STRING";
            case TIME:
            case TIME_WITH_TIME_ZONE:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case TIMESTAMP_WITH_TIME_ZONE:
                return "TIMESTAMPTZ";
            case TINYINT:
                return "SMALLINT";
            case UNKNOWN:
                throw new IllegalArgumentException("Can't map an unknown type");
            case VARBINARY:
                return "VARBINARY";
            case VARCHAR:
                if (!(type instanceof VarcharType)) {
                    throw new IllegalArgumentException("Expected varchar type but was " + type.getClass().getName());
                }
                final VarcharType varcharType = (VarcharType) type;
                return "VARCHAR(" + varcharType.getLength() + ")";
            default:
                throw new IllegalArgumentException("Unknown type " + type.getTypeSignature().getBase());
        }
    }
}
