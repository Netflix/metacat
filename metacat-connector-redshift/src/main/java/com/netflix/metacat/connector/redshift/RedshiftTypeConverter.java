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
package com.netflix.metacat.connector.redshift;

import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarcharType;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Type converter for Redshift.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Slf4j
public class RedshiftTypeConverter extends JdbcTypeConverter {

    static final int DEFAULT_CHARACTER_LENGTH = 256;
    private static final String DEFAULT_CHARACTER_LENGTH_STRING = Integer.toString(DEFAULT_CHARACTER_LENGTH);

    /**
     * {@inheritDoc}
     *
     * @see <a href="http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html">Redshift Types</a>
     * @see <a href="http://docs.aws.amazon.com/redshift/latest/dg/c_unsupported-postgresql-datatypes.html">
     *     Unsupported PostgreSQL Types
     *     </a>
     */
    @Override
    public Type toMetacatType(@Nonnull @NonNull final String type) {
        // See: https://www.postgresql.org/docs/current/static/datatype.html
        final String lowerType = type.toLowerCase();

        // Split up the possible type: TYPE[(size, magnitude)] EXTRA
        final String[] splitType = this.splitType(lowerType);
        switch (splitType[0]) {
            case "smallint":
            case "int2":
                return BaseType.SMALLINT;
            case "int":
            case "integer":
            case "int4":
                return BaseType.INT;
            case "int8":
            case "bigint":
                return BaseType.BIGINT;
            case "decimal":
            case "numeric":
                return this.toMetacatDecimalType(splitType);
            case "real":
            case "float4":
                return BaseType.FLOAT;
            case "double precision":
            case "float8":
            case "float":
                return BaseType.DOUBLE;
            case "character varying":
            case "varchar":
            case "nvarchar":
                return this.toMetacatVarcharType(splitType);
            case "text":
                // text is basically alias for VARCHAR(256)
                splitType[1] = DEFAULT_CHARACTER_LENGTH_STRING;
                return this.toMetacatVarcharType(splitType);
            case "character":
            case "char":
            case "nchar":
                return this.toMetacatCharType(splitType);
            case "bpchar":
                // bpchar defaults to fixed length of 256 characters
                splitType[1] = DEFAULT_CHARACTER_LENGTH_STRING;
                return this.toMetacatCharType(splitType);
            case "timestamp":
                return this.toMetacatTimestampType(splitType);
            case "timestampz":
                return BaseType.TIMESTAMP_WITH_TIME_ZONE;
            case "date":
                return BaseType.DATE;
            case "boolean":
            case "bool":
                return BaseType.BOOLEAN;
            default:
                // see: http://docs.aws.amazon.com/redshift/latest/dg/c_unsupported-postgresql-datatypes.html
                throw new IllegalArgumentException("Unhandled or unknown Redshift type " + splitType[0]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String fromMetacatType(@Nonnull @NonNull final Type type) {
        switch (type.getTypeSignature().getBase()) {
            case ARRAY:
                throw new UnsupportedOperationException("Redshift doesn't support array types");
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
                return "DECIMAL(" + decimalType.getPrecision() + ", " + decimalType.getScale() + ")";
            case DOUBLE:
            case FLOAT:
                return "DOUBLE PRECISION";
            case INT:
                return "INT";
            case INTERVAL_DAY_TO_SECOND:
                throw new UnsupportedOperationException("Redshift doesn't support interval types");
            case INTERVAL_YEAR_TO_MONTH:
                throw new UnsupportedOperationException("Redshift doesn't support interval types");
            case JSON:
                throw new UnsupportedOperationException("Redshift doesn't support JSON types");
            case MAP:
                throw new UnsupportedOperationException("Redshift doesn't support MAP types");
            case ROW:
                throw new UnsupportedOperationException("Redshift doesn't support ROW types");
            case SMALLINT:
                return "SMALLINT";
            case STRING:
                throw new UnsupportedOperationException("Redshift doesn't support STRING types");
            case TIME:
            case TIME_WITH_TIME_ZONE:
                throw new UnsupportedOperationException("Redshift doesn't support TIME types");
            case TIMESTAMP:
                return "TIMESTAMP";
            case TIMESTAMP_WITH_TIME_ZONE:
                return "TIMESTAMPZ";
            case TINYINT:
                // NOTE: There is no tiny int type in Redshift so using slightly larger SMALLINT
                return "SMALLINT";
            case UNKNOWN:
                throw new IllegalArgumentException("Can't map an unknown type");
            case VARBINARY:
                throw new UnsupportedOperationException("Redshift doesn't support VARBINARY types");
            case VARCHAR:
                if (!(type instanceof VarcharType)) {
                    throw new IllegalArgumentException("Expected varchar type but was " + type.getClass().getName());
                }
                final VarcharType varcharType = (VarcharType) type;
                // NOTE: PostgreSQL lets you store up to 1GB in a varchar field which is about the same as TEXT
                return "VARCHAR(" + varcharType.getLength() + ")";
            default:
                throw new IllegalArgumentException("Unknown type " + type.getTypeSignature().getBase());
        }
    }
}
