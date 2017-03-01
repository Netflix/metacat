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
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarbinaryType;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Type converter for PostgreSql.
 *
 * @author tgianos
 * @since 0.1.52
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
                elementType = BaseType.SMALLINT;
                break;
            case "int":
                elementType = BaseType.INT;
                break;
            case "bigint":
                elementType = BaseType.BIGINT;
                break;
            case "decimal":
            case "numeric":
                elementType = this.toMetacatDecimalType(splitType);
                break;
            case "real":
                elementType = BaseType.FLOAT;
                break;
            case "double precision":
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
            case "date":
                elementType = BaseType.DATE;
                break;
            case "time":
                elementType = this.toMetacatTimeType(splitType);
                break;
            case "boolean":
                elementType = BaseType.BOOLEAN;
                break;
            case "bit":
            case "bit varying":
                elementType = this.toMetacatBitType(splitType);
                break;
            case "json":
                elementType = BaseType.JSON;
                break;
            case "smallserial":
            case "serial":
            case "bigserial":
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
                throw new UnsupportedOperationException("Encountered " + splitType[0] + " type. Ignoring");
            default:
                // TODO: Will catch complex types but not sure how to parse beyond that right now, may be recursive?
                // https://www.postgresql.org/docs/current/static/rowtypes.html
                throw new IllegalArgumentException("Unhandled or unknown sql type" + splitType[0]);
        }

        return this.checkForArray(splitType, elementType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String fromMetacatType(@Nonnull @NonNull final Type type) {
        return null;
    }

    private Type checkForArray(@Nonnull @NonNull final String[] splitType, @Nonnull @NonNull final Type elementType) {
        if (SINGLE_ARRAY.equals(splitType[1]) || ARRAY.equals(splitType[4])) {
            return new ArrayType(elementType);
        } else if (MULTI_ARRAY.equals(splitType[1])) {
            return new ArrayType(new ArrayType(elementType));
        } else {
            return elementType;
        }
    }
}
