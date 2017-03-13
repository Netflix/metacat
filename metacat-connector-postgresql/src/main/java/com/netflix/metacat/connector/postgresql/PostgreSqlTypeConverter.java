/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.connector.postgresql;

import com.netflix.metacat.common.type.ArrayType;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarbinaryType;
import com.netflix.metacat.common.type.VarcharType;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Type converter for PostgreSql.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Slf4j
public class PostgreSqlTypeConverter extends JdbcTypeConverter {

    private static final String ARRAY = "array";
    private static final String SINGLE_ARRAY = "[]";
    private static final String MULTI_ARRAY = "[][]";

    /**
     * {@inheritDoc}
     *
     * @see <a href="https://www.postgresql.org/docs/current/static/datatype.html">PosgreSQL Data Types</a>
     */
    @Override
    public Type toMetacatType(@Nonnull @NonNull final String type) {
        // See: https://www.postgresql.org/docs/current/static/datatype.html
        final String lowerType = type.toLowerCase();

        // Split up the possible type: TYPE[(size, magnitude)] EXTRA
        final String[] splitType = this.splitType(lowerType);
        final Type elementType;
        switch (splitType[0]) {
            case "smallint":
            case "int2":
                elementType = BaseType.SMALLINT;
                break;
            case "int":
            case "integer":
            case "int4":
                elementType = BaseType.INT;
                break;
            case "int8":
            case "bigint":
                elementType = BaseType.BIGINT;
                break;
            case "decimal":
            case "numeric":
                elementType = this.toMetacatDecimalType(splitType);
                break;
            case "real":
            case "float4":
                elementType = BaseType.FLOAT;
                break;
            case "double precision":
            case "float8":
                elementType = BaseType.DOUBLE;
                break;
            case "character varying":
            case "varchar":
                elementType = this.toMetacatVarcharType(splitType);
                break;
            case "character":
            case "char":
                elementType = this.toMetacatCharType(splitType);
                break;
            case "text":
                elementType = BaseType.STRING;
                break;
            case "bytea":
                elementType = VarbinaryType.createVarbinaryType(Integer.MAX_VALUE);
                break;
            case "timestamp":
                elementType = this.toMetacatTimestampType(splitType);
                break;
            case "timestampz":
                elementType = BaseType.TIMESTAMP_WITH_TIME_ZONE;
                break;
            case "date":
                elementType = BaseType.DATE;
                break;
            case "time":
                elementType = this.toMetacatTimeType(splitType);
                break;
            case "timez":
                elementType = BaseType.TIME_WITH_TIME_ZONE;
                break;
            case "boolean":
            case "bool":
                elementType = BaseType.BOOLEAN;
                break;
            case "bit":
            case "bit varying":
            case "varbit":
                elementType = this.toMetacatBitType(splitType);
                break;
            case "json":
                elementType = BaseType.JSON;
                break;
            case "smallserial":
            case "serial2":
            case "serial":
            case "serial4":
            case "bigserial":
            case "serial8":
            case "money":
            case "interval":
            case "enum":
            case "point":
            case "line":
            case "lseg":
            case "box":
            case "path":
            case "polygon":
            case "circle":
            case "cidr":
            case "inet":
            case "macaddr":
            case "tsvector":
            case "tsquery":
            case "uuid":
            case "xml":
            case "int4range":
            case "int8range":
            case "numrange":
            case "tsrange":
            case "tstzrange":
            case "daterange":
            case "oid":
            case "regproc":
            case "regprocedure":
            case "regoper":
            case "regoperator":
            case "regclass":
            case "regtype":
            case "regrole":
            case "regnamespace":
            case "regconfig":
            case "regdictionary":
            case "pg_lsn":
            case "jsonb":
            case "txid_snapshot":
                throw new UnsupportedOperationException("Encountered " + splitType[0] + " type. Ignoring");
            default:
                // TODO: Will catch complex types but not sure how to parse beyond that right now, may be recursive?
                // https://www.postgresql.org/docs/current/static/rowtypes.html
                throw new IllegalArgumentException("Unhandled or unknown sql type " + splitType[0]);
        }

        return this.checkForArray(splitType, elementType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String fromMetacatType(@Nonnull @NonNull final Type type) {
        switch (type.getTypeSignature().getBase()) {
            case ARRAY:
                if (!(type instanceof ArrayType)) {
                    throw new IllegalArgumentException("Expected an ARRAY type but was " + type.getClass().getName());
                }
                final ArrayType arrayType = (ArrayType) type;
                final String array;
                final Type elementType;
                // Check for nested arrays
                if (arrayType.getElementType() instanceof ArrayType) {
                    array = MULTI_ARRAY;
                    elementType = ((ArrayType) arrayType.getElementType()).getElementType();
                } else {
                    array = SINGLE_ARRAY;
                    elementType = arrayType.getElementType();
                }

                // Recursively determine the type of the array
                return this.fromMetacatType(elementType) + array;
            case BIGINT:
                return "BIGINT";
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
                return "NUMERIC(" + decimalType.getPrecision() + ", " + decimalType.getScale() + ")";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case FLOAT:
                return "REAL";
            case INT:
                return "INT";
            case INTERVAL_DAY_TO_SECOND:
                // TODO: It does but not sure how best to represent now
                throw new UnsupportedOperationException("PostgreSQL doesn't support interval types");
            case INTERVAL_YEAR_TO_MONTH:
                // TODO: It does but not sure how best to represent now
                throw new UnsupportedOperationException("PostgreSQL doesn't support interval types");
            case JSON:
                return "JSON";
            case MAP:
                throw new UnsupportedOperationException("PostgreSQL doesn't support map types");
            case ROW:
                // TODO: Well it does but how do we know what the internal type is?
                throw new UnsupportedOperationException("PostgreSQL doesn't support row types");
            case SMALLINT:
                return "SMALLINT";
            case STRING:
                return "TEXT";
            case TIME:
                return "TIME";
            case TIME_WITH_TIME_ZONE:
                return "TIME WITH TIME ZONE";
            case TIMESTAMP:
                return "TIMESTAMP";
            case TIMESTAMP_WITH_TIME_ZONE:
                return "TIMESTAMP WITH TIME ZONE";
            case TINYINT:
                // NOTE: There is no tiny int type in PostgreSQL so using slightly larger SMALLINT
                return "SMALLINT";
            case UNKNOWN:
                throw new IllegalArgumentException("Can't map an unknown type");
            case VARBINARY:
                return "BYTEA";
            case VARCHAR:
                if (!(type instanceof VarcharType)) {
                    throw new IllegalArgumentException("Expected varchar type but was " + type.getClass().getName());
                }
                final VarcharType varcharType = (VarcharType) type;
                // NOTE: PostgreSQL lets you store up to 1GB in a varchar field which is about the same as TEXT
                return "CHARACTER VARYING(" + varcharType.getLength() + ")";
            default:
                throw new IllegalArgumentException("Unknown type " + type.getTypeSignature().getBase());
        }
    }

    private Type checkForArray(@Nonnull @NonNull final String[] splitType, @Nonnull @NonNull final Type elementType) {
        if (SINGLE_ARRAY.equals(splitType[3]) || ARRAY.equals(splitType[4])) {
            return new ArrayType(elementType);
        } else if (MULTI_ARRAY.equals(splitType[3])) {
            return new ArrayType(new ArrayType(elementType));
        } else {
            return elementType;
        }
    }
}
