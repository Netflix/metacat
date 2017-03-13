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
package com.netflix.metacat.connector.cassandra

import com.google.common.collect.Lists
import com.netflix.metacat.common.type.*
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the CassandraTypeConverter.
 *
 * @author tgianos
 * @since 1.0.0
 */
class CassandraTypeConverterSpec extends Specification {

    def converter = new CassandraTypeConverter()

    @Unroll
    "Can convert Cassandra string: #type to Metacat Type: #signature"() {

        expect:
        this.converter.toMetacatType(type).getTypeSignature().toString() == signature

        where:
        type                                         | signature
        "ASCII"                                      | BaseType.STRING.getTypeSignature().toString()
        "BIGINT"                                     | BaseType.BIGINT.getTypeSignature().toString()
        "blOb"                                       | VarbinaryType
            .createVarbinaryType(Integer.MAX_VALUE)
            .getTypeSignature()
            .toString()
        "BOOLEAN"                                    | BaseType.BOOLEAN.getTypeSignature().toString()
        "counter"                                    | BaseType.BIGINT.getTypeSignature().toString()
        "DATE"                                       | BaseType.DATE.getTypeSignature().toString()
        "decimal"                                    | DecimalType.createDecimalType().getTypeSignature().toString()
        "DOUBLE"                                     | BaseType.DOUBLE.getTypeSignature().toString()
        "float"                                      | BaseType.FLOAT.getTypeSignature().toString()
        "frozen <INT>"                               | BaseType.INT.getTypeSignature().toString()
        "frozen <map <text, int>>"                   | new MapType(BaseType.STRING, BaseType.INT)
            .getTypeSignature()
            .toString()
        "INT"                                        | BaseType.INT.getTypeSignature().toString()
        "LIST<TEXT>"                                 | new ArrayType(BaseType.STRING).getTypeSignature().toString()
        "map <int, frozen <map<text, int>>>"         | new MapType(
            BaseType.INT, new MapType(BaseType.STRING, BaseType.INT)
        )
            .getTypeSignature()
            .toString()
        "sMaLLInt"                                   | BaseType.SMALLINT.getTypeSignature().toString()
        "TEXT"                                       | BaseType.STRING.getTypeSignature().toString()
        "time"                                       | BaseType.TIME.getTypeSignature().toString()
        "timestamp"                                  | BaseType.TIMESTAMP.getTypeSignature().toString()
        "tinyint"                                    | BaseType.TINYINT.getTypeSignature().toString()
        "tuple<map<text, double>, list<int>, float>" | new RowType(
            Lists.newArrayList(
                new RowType.RowField(new MapType(BaseType.STRING, BaseType.DOUBLE), null),
                new RowType.RowField(new ArrayType(BaseType.INT), null),
                new RowType.RowField(BaseType.FLOAT, null)
            )
        ).getTypeSignature().toString()
        "varchar"                                    | BaseType.STRING.getTypeSignature().toString()
        "VARINT"                                     | BaseType.INT.getTypeSignature().toString()
    }

    @Unroll
    "Can't process unsupported type #type"() {

        when:
        this.converter.toMetacatType(type)

        then:
        thrown exception

        where:
        type       | exception
        "INET"     | UnsupportedOperationException
        "SET"      | UnsupportedOperationException
        "TIMEUUID" | UnsupportedOperationException
        "UUID"     | UnsupportedOperationException
        "UserType" | UnsupportedOperationException
    }

    @Unroll
    "Can convert type #type to Cassandra CQL type string #cql"() {

        expect:
        this.converter.fromMetacatType(type) == cql

        where:
        type                                                      | cql
        new ArrayType(BaseType.INT)                               | "list<int>"
        new ArrayType(new MapType(BaseType.INT, BaseType.STRING)) | "list<frozen map<int, text>>"
        BaseType.BIGINT                                           | "bigint"
        BaseType.BOOLEAN                                          | "boolean"
        new CharType(10)                                          | "text"
        BaseType.DATE                                             | "date"
        DecimalType.createDecimalType()                           | "decimal"
        BaseType.DOUBLE                                           | "double"
        BaseType.FLOAT                                            | "float"
        BaseType.INT                                              | "int"
        new MapType(
            new ArrayType(BaseType.DOUBLE),
            new MapType(BaseType.INT, BaseType.BOOLEAN)
        )                                                         | "map<frozen list<double>, frozen map<int, boolean>>"
        new RowType(
            Lists.newArrayList(
                new RowType.RowField(new MapType(BaseType.STRING, BaseType.DOUBLE), null),
                new RowType.RowField(new ArrayType(BaseType.INT), null),
                new RowType.RowField(BaseType.FLOAT, null)
            )
        )                                                         | "tuple<map<text, double>, list<int>, float>"
        BaseType.SMALLINT                                         | "smallint"
        BaseType.STRING                                           | "text"
        BaseType.TIME                                             | "time"
        BaseType.TIMESTAMP                                        | "timestamp"
        BaseType.TINYINT                                          | "tinyint"
        VarbinaryType.createVarbinaryType(10)                     | "blob"
        VarcharType.createVarcharType(10)                         | "text"
    }

    @Unroll
    "Can't convert type #type back to CQL"() {
        when:
        this.converter.fromMetacatType(type)

        then:
        thrown exception

        where:
        type                              | exception
        BaseType.INTERVAL_DAY_TO_SECOND   | UnsupportedOperationException
        BaseType.INTERVAL_YEAR_TO_MONTH   | UnsupportedOperationException
        BaseType.JSON                     | UnsupportedOperationException
        BaseType.TIME_WITH_TIME_ZONE      | UnsupportedOperationException
        BaseType.TIMESTAMP_WITH_TIME_ZONE | UnsupportedOperationException
        BaseType.UNKNOWN                  | UnsupportedOperationException
    }
}
