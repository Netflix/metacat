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
package com.netflix.metacat.connector.cassandra;

import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.type.ArrayType;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.MapType;
import com.netflix.metacat.common.type.RowType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarbinaryType;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data type converter for Cassandra.
 *
 * @see <a href="http://cassandra.apache.org/doc/latest/cql/types.html">Cassandra Data Types</a>
 * @author tgianos
 * @since 1.0.0
 */
public class CassandraTypeConverter implements ConnectorTypeConverter {

    private static final Pattern TYPE_PATTERN = Pattern.compile("^\\s*?(\\w*)\\s*?(?:<\\s*?(.*)\\s*?>)?\\s*?$");
    private static final int TYPE_GROUP = 1;
    private static final int PARAM_GROUP = 2;

    private static final Pattern MAP_PARAM_PATTERN = Pattern
        .compile("^\\s*?((?:frozen\\s*?)?\\w*\\s*?(?:<.*>)?)\\s*?,\\s*?((?:frozen\\s*?)?\\w*\\s*?(?:<.*>)?)\\s*?$");
    private static final int MAP_KEY_GROUP = 1;
    private static final int MAP_VALUE_GROUP = 2;

    private static final Pattern TUPLE_PARAM_PATTERN
        = Pattern.compile("(?:(\\w[\\w\\s]+(?:<[\\w+,\\s]+>\\s*?)?),?\\s*?)");
    private static final int TUPLE_GROUP = 1;

    /**
     * {@inheritDoc}
     */
    @Override
    public Type toMetacatType(@Nonnull @NonNull final String type) {
        final Matcher matcher = TYPE_PATTERN.matcher(type.toLowerCase());
        // TODO: Escape case from recursion may be needed to avoid potential infinite
        if (matcher.matches()) {
            final String cqlType = matcher.group(TYPE_GROUP);
            switch (cqlType) {
                case "ascii":
                    return BaseType.STRING;
                case "bigint":
                    return BaseType.BIGINT;
                case "blob":
                    return VarbinaryType.createVarbinaryType(Integer.MAX_VALUE);
                case "boolean":
                    return BaseType.BOOLEAN;
                case "counter":
                    return BaseType.BIGINT;
                case "date":
                    return BaseType.DATE;
                case "decimal":
                    return DecimalType.createDecimalType();
                case "double":
                    return BaseType.DOUBLE;
                case "float":
                    return BaseType.FLOAT;
                case "frozen":
                    return this.toMetacatType(matcher.group(PARAM_GROUP));
                case "inet":
                    throw new UnsupportedOperationException("INET CQL types not currently supported");
                case "int":
                    return BaseType.INT;
                case "list":
                    // The possible null for the PARAM_GROUP should be handled on recursive call throwing exception
                    return new ArrayType(this.toMetacatType(matcher.group(PARAM_GROUP)));
                case "map":
                    final Matcher mapMatcher = MAP_PARAM_PATTERN.matcher(matcher.group(PARAM_GROUP));
                    if (mapMatcher.matches()) {
                        return new MapType(
                            this.toMetacatType(mapMatcher.group(MAP_KEY_GROUP)),
                            this.toMetacatType(mapMatcher.group(MAP_VALUE_GROUP))
                        );
                    } else {
                        throw new IllegalArgumentException("Unable to parse map params " + matcher.group(PARAM_GROUP));
                    }
                case "set":
                    throw new UnsupportedOperationException("SET CQL types not currently supported");
                case "smallint":
                    return BaseType.SMALLINT;
                case "text":
                    return BaseType.STRING;
                case "time":
                    return BaseType.TIME;
                case "timestamp":
                    return BaseType.TIMESTAMP;
                case "timeuuid":
                    throw new UnsupportedOperationException("TIMEUUID CQL types not currently supported");
                case "tinyint":
                    return BaseType.TINYINT;
                case "tuple":
                    if (matcher.group(PARAM_GROUP) == null) {
                        throw new IllegalArgumentException("Empty tuple param group. Unable to parse");
                    }
                    final Matcher tupleMatcher = TUPLE_PARAM_PATTERN.matcher(matcher.group(PARAM_GROUP));
                    final ImmutableList.Builder<RowType.RowField> tupleFields = ImmutableList.builder();
                    while (tupleMatcher.find()) {
                        tupleFields.add(
                            new RowType.RowField(
                                this.toMetacatType(tupleMatcher.group(TUPLE_GROUP)),
                                null
                            )
                        );
                    }
                    return new RowType(tupleFields.build());
                case "uuid":
                    throw new UnsupportedOperationException("UUID CQL types not currently supported");
                case "varchar":
                    return BaseType.STRING;
                case "varint":
                    return BaseType.INT;
                default:
                    throw new UnsupportedOperationException("Unhandled type " + cqlType);
            }
        } else {
            throw new IllegalArgumentException("Unable to parse CQL type " + type);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String fromMetacatType(@Nonnull @NonNull final Type type) {
        switch (type.getTypeSignature().getBase()) {
            case ARRAY:
                if (!(type instanceof ArrayType)) {
                    throw new IllegalArgumentException("Expected an ArrayType and got " + type.getClass());
                }
                final ArrayType arrayType = (ArrayType) type;
                return "list<" + this.getElementTypeString(arrayType.getElementType()) + ">";
            case BIGINT:
                return "bigint";
            case BOOLEAN:
                return "boolean";
            case CHAR:
                // TODO: Should we make this unsupported?
                return "text";
            case DATE:
                return "date";
            case DECIMAL:
                return "decimal";
            case DOUBLE:
                return "double";
            case FLOAT:
                return "float";
            case INT:
                return "int";
            case INTERVAL_DAY_TO_SECOND:
                throw new UnsupportedOperationException("Cassandra doesn't support intervals.");
            case INTERVAL_YEAR_TO_MONTH:
                throw new UnsupportedOperationException("Cassandra doesn't support intervals.");
            case JSON:
                throw new UnsupportedOperationException("Cassandra doesn't support JSON natively.");
            case MAP:
                if (!(type instanceof MapType)) {
                    throw new IllegalArgumentException("Was expecting MapType instead it is " + type.getClass());
                }
                final MapType mapType = (MapType) type;
                final Type keyType = mapType.getKeyType();
                final Type valueType = mapType.getValueType();
                return "map<" + this.getElementTypeString(keyType) + ", " + this.getElementTypeString(valueType) + ">";
            case ROW:
                if (!(type instanceof RowType)) {
                    throw new IllegalArgumentException("Was expecting RowType instead it is " + type.getClass());
                }
                final RowType rowType = (RowType) type;
                final StringBuilder tupleBuilder = new StringBuilder();
                tupleBuilder.append("tuple<");
                // Tuple fields don't need to be frozen
                boolean putComma = false;
                for (final RowType.RowField field : rowType.getFields()) {
                    if (putComma) {
                        tupleBuilder.append(", ");
                    } else {
                        putComma = true;
                    }
                    tupleBuilder.append(this.fromMetacatType(field.getType()));
                }
                tupleBuilder.append(">");
                return tupleBuilder.toString();
            case SMALLINT:
                return "smallint";
            case STRING:
                return "text";
            case TIME:
                return "time";
            case TIME_WITH_TIME_ZONE:
                throw new UnsupportedOperationException("Cassandra doesn't support time with timezone");
            case TIMESTAMP:
                return "timestamp";
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException("Cassandra doesn't support time with timezone");
            case TINYINT:
                return "tinyint";
            case UNKNOWN:
                throw new UnsupportedOperationException("Cassandra doesn't support an unknown type");
            case VARBINARY:
                return "blob";
            case VARCHAR:
                return "text";
            default:
                throw new IllegalArgumentException("Unknown type: " + type.getTypeSignature().getBase());
        }
    }

    private String getElementTypeString(final Type elementType) {
        // Nested collections must have
        if (elementType instanceof MapType || elementType instanceof ArrayType) {
            return "frozen " + this.fromMetacatType(elementType);
        } else {
            return this.fromMetacatType(elementType);
        }
    }
}
