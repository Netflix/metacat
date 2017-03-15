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

import com.datastax.driver.core.DataType
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
        this.converter.toMetacatType(type) == metacatType

        where:
        type                                                            | metacatType
        DataType.ascii().toString()                                     | BaseType.STRING
        DataType.bigint().toString()                                    | BaseType.BIGINT
        DataType.blob().toString()                                      | VarbinaryType.createVarbinaryType(Integer.MAX_VALUE)
        DataType.cboolean().toString()                                  | BaseType.BOOLEAN
        DataType.counter().toString()                                   | BaseType.BIGINT
        DataType.date().toString()                                      | BaseType.DATE
        DataType.decimal().toString()                                   | DecimalType.createDecimalType()
        DataType.cdouble().toString()                                   | BaseType.DOUBLE
        DataType.cfloat().toString()                                    | BaseType.FLOAT
        DataType.frozenMap(DataType.text(), DataType.cint()).toString() | new MapType(BaseType.STRING, BaseType.INT)
        DataType.cint().toString()                                      | BaseType.INT
        DataType.list(DataType.text()).toString()                       | new ArrayType(BaseType.STRING)
        DataType.map(
            DataType.cint(),
            DataType.frozenMap(
                DataType.text(),
                DataType.cint()
            )
        ).toString()                                                    | new MapType(
            BaseType.INT,
            new MapType(
                BaseType.STRING,
                BaseType.INT
            )
        )
        DataType.smallint().toString()                                  | BaseType.SMALLINT
        DataType.text().toString()                                      | BaseType.STRING
        DataType.time().toString()                                      | BaseType.TIME
        DataType.timestamp().toString()                                 | BaseType.TIMESTAMP
        DataType.tinyint().toString()                                   | BaseType.TINYINT
        "tuple<map<text, double>, list<int>, float>"                    | new RowType(
            Lists.newArrayList(
                new RowType.RowField(new MapType(BaseType.STRING, BaseType.DOUBLE), "field0"),
                new RowType.RowField(new ArrayType(BaseType.INT), "field1"),
                new RowType.RowField(BaseType.FLOAT, "field2")
            )
        )
        DataType.varchar().toString()                                   | BaseType.STRING
        DataType.varint().toString()                                    | BaseType.INT
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
                new RowType.RowField(new MapType(BaseType.STRING, BaseType.DOUBLE), "field0"),
                new RowType.RowField(new ArrayType(BaseType.INT), "field1"),
                new RowType.RowField(BaseType.FLOAT, "field2")
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
