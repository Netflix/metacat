/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive.converters

import com.netflix.metacat.common.server.properties.Config
import org.apache.iceberg.PartitionField
import org.apache.iceberg.transforms.Identity
import org.apache.iceberg.transforms.VoidTransform
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for hive and canonical type converter.
 * @author zhenl
 * @since 1.0.0
 */
class HiveTypeConverterSpec extends Specification {
    HiveTypeConverter converter;

    def setup() {
        this.converter = new HiveTypeConverter()
    }

    @Unroll
    def 'can convert "#typeString" to a presto type and back'(String typeString) {
        expect:
        def metacatType = converter.toMetacatType(typeString)
        def hiveType = converter.fromMetacatType(metacatType)
        def metacatTypeFromHiveType = converter.toMetacatType(hiveType)
        metacatTypeFromHiveType ==  metacatType
        where:
        typeString << [
            'tinyint',
            'smallint',
            'int',
            'bigint',
            'float',
            'double',
            'decimal',
            'decimal(4,2)',
            'array<decimal(4,2)>',
            'timestamp',
            'date',
            'string',
            'varchar(10)',
            'char(10)',
            'boolean',
            'binary',
            'array<bigint>',
            'array<boolean>',
            'array<double>',
            'array<bigint>',
            'array<string>',
            'array<map<bigint,bigint>>',
            'array<map<bigint,string>>',
            'array<map<string,bigint>>',
            'array<map<string,string>>',
            'array<struct<field1:bigint,field2:bigint>>',
            'array<struct<field1:bigint,field2:string>>',
            'array<struct<field1:string,field2:bigint>>',
            'array<struct<field1:string,field2:string>>',
            'map<boolean,boolean>',
            'map<boolean,string>',
            'map<bigint,bigint>',
            'map<string,double>',
            'map<string,bigint>',
            'map<string,string>',
            'map<string,struct<field1:array<bigint>>>',
            'struct<field1:bigint,field2:bigint,field3:bigint>',
            'struct<field1:bigint,field2:string,field3:double>',
            'struct<field1:bigint,field2:string,field3:string>',
            'struct<field1:string,field2:bigint,field3:bigint>',
            'struct<field1:string,field2:string,field3:bigint>',
            'struct<field1:string,field2:string,field3:string>',

            "decimal",
            "decimal( 38)",
            "decimal( 38, 9)",
            "decimal(38, 9)",
            "decimal(38,  9)",
            "decimal(38,9)",
            "decimal(38,9 )",
            "decimal (38,9 )",
            "decimal (38)",
            "decimal(38 )",
            "array<decimal>",
            "array<decimal(38, 9)>",
            "array<decimal(38,  9)>",
            "array<decimal(38,9)>",
            "array<decimal(38,9 )>",
            "array<decimal (38,9 )>",
            "array<decimal (38)>",
            "array<decimal(38 )>",
            "struct<field1:string,field2:decimal,field3:bigint>",
            "struct<field1:string,field2:decimal(38, 9),field3:bigint>",
            "struct<field1:string,field2:decimal( 38, 9),field3:bigint>",
            "struct<field1:string,field2:decimal(38,  9),field3:bigint>",
            "struct<field1:string,field2:decimal(38,9),field3:bigint>",
            "struct<field1:string,field2:decimal(38,9 ),field3:bigint>",
            "struct<field1:string,field2:decimal (38,9 ),field3:bigint>",
            "struct<field1:string,field2:decimal (38),field3:bigint>",
            "struct<field1:string,field2:decimal(38 ),field3:bigint>",

            "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>",
            "struct<prediction_date:int,lower_confidence_amt:decimal(30, 2),upper_confidence_amt:decimal(30,2),model_short_name:string>",
            "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30, 2),model_short_name:string>",
            "struct<prediction_date:int,lower_confidence_amt:decimal(30, 2),upper_confidence_amt:decimal(30, 2),model_short_name:string>",

            "struct<prediction_cnt:int,first_prediction_date:int,last_prediction_date:int>",
            "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>",
            "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2)>",
            "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string,compression_factor:double>",
            "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string,pmvs_sticker_pts:double,pmvs_sticker_pts_lower_confidence_amt:double,pmvs_sticker_pts_upper_confidence_amt:double,pmvs_dt_pts:double,pmvs_dt_pct:double,pmvs_baseline_dt_pct:double,thumber_cnt:int,thumber_threshold_met:boolean,pmvs_dt_pts_ignoring_threshold:double,pmvs_dt_rmse:double,pmvs_baseline_dt_rmse:double>",
            "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string>",
            "struct<prediction_date:int,lower_confidence_amt:int,upper_confidence_amt:int,model_short_name:string>",
            "struct<prediction_date:int,prediction_source:string>",

            // Nested Type with UpperCase
            'array<struct<date:string,countryCodes:array<string>,source:string>>',
            "struct<Field3:struct<Nested_Field1:bigint,Nested_Field2:bigint>>",
            "struct<Field1:bigint,Field2:bigint,field3:struct<NESTED_Field1:bigint,NesteD_Field2:bigint>>"
        ]
    }

    @Unroll
    def "handles sanitization of decimal declarations with spaces"() {
        when:
        def sanitizedType = HiveTypeConverter.sanitizeType(input)

        then:
        sanitizedType == expected

        where:
        input                                                         || expected
        "tinyint"                                                     || "tinyint"
        "smallint"                                                    || "smallint"
        "int"                                                         || "int"
        "bigint"                                                      || "bigint"
        "float"                                                       || "float"
        "double"                                                      || "double"
        "decimal"                                                     || "decimal"
        "decimal(4,2)"                                                || "decimal(4,2)"
        "array<decimal(4,2)>"                                         || "array<decimal(4,2)>"
        "timestamp"                                                   || "timestamp"
        "date"                                                        || "date"
        "string"                                                      || "string"
        "varchar(10)"                                                 || "varchar(10)"
        "char(10)"                                                    || "char(10)"
        "boolean"                                                     || "boolean"
        "binary"                                                      || "binary"
        "array<bigint>"                                               || "array<bigint>"
        "array<boolean>"                                              || "array<boolean>"
        "array<double>"                                               || "array<double>"
        "array<bigint>"                                               || "array<bigint>"
        "array<string>"                                               || "array<string>"
        "array<map<bigint,bigint>>"                                   || "array<map<bigint,bigint>>"
        "array<map<bigint,string>>"                                   || "array<map<bigint,string>>"
        "array<map<string,bigint>>"                                   || "array<map<string,bigint>>"
        "array<map<string,string>>"                                   || "array<map<string,string>>"
        "array<struct<field1:bigint,field2:bigint>>"                  || "array<struct<field1:bigint,field2:bigint>>"
        "array<struct<field1:bigint,field2:string>>"                  || "array<struct<field1:bigint,field2:string>>"
        "array<struct<field1:string,field2:bigint>>"                  || "array<struct<field1:string,field2:bigint>>"
        "array<struct<field1:string,field2:string>>"                  || "array<struct<field1:string,field2:string>>"
        "map<boolean,boolean>"                                        || "map<boolean,boolean>"
        "map<boolean,string>"                                         || "map<boolean,string>"
        "map<bigint,bigint>"                                          || "map<bigint,bigint>"
        "map<string,double>"                                          || "map<string,double>"
        "map<string,bigint>"                                          || "map<string,bigint>"
        "map<string,string>"                                          || "map<string,string>"
        "map<string,struct<field1:array<bigint>>>"                    || "map<string,struct<field1:array<bigint>>>"
        "struct<field1:bigint,field2:bigint,field3:bigint>"           || "struct<field1:bigint,field2:bigint,field3:bigint>"
        "struct<field1:bigint,field2:string,field3:double>"           || "struct<field1:bigint,field2:string,field3:double>"
        "struct<field1:bigint,field2:string,field3:string>"           || "struct<field1:bigint,field2:string,field3:string>"
        "struct<field1:string,field2:bigint,field3:bigint>"           || "struct<field1:string,field2:bigint,field3:bigint>"
        "struct<field1:string,field2:string,field3:bigint>"           || "struct<field1:string,field2:string,field3:bigint>"
        "struct<field1:string,field2:string,field3:string>"           || "struct<field1:string,field2:string,field3:string>"

        // problem types starts here; we expect the spaces in the decimal declarations to be removed
        "decimal"                                                     || "decimal"
        "decimal( 38)"                                                || "decimal(38)"
        "decimal( 38, 9)"                                             || "decimal(38,9)"
        "decimal(38, 9)"                                              || "decimal(38,9)"
        "decimal(38,  9)"                                             || "decimal(38,9)"
        "decimal(38,9)"                                               || "decimal(38,9)"
        "decimal(38,9 )"                                              || "decimal(38,9)"
        "decimal (38,9 )"                                             || "decimal(38,9)"
        "decimal (38)"                                                || "decimal(38)"
        "decimal(38 )"                                                || "decimal(38)"
        "array<decimal>"                                              || "array<decimal>"
        "array<decimal(38, 9)>"                                       || "array<decimal(38,9)>"
        "array<decimal(38,  9)>"                                      || "array<decimal(38,9)>"
        "array<decimal(38,9)>"                                        || "array<decimal(38,9)>"
        "array<decimal(38,9 )>"                                       || "array<decimal(38,9)>"
        "array<decimal (38,9 )>"                                      || "array<decimal(38,9)>"
        "array<decimal (38)>"                                         || "array<decimal(38)>"
        "array<decimal(38 )>"                                         || "array<decimal(38)>"
        "struct<field1:string,field2:decimal,field3:bigint>"          || "struct<field1:string,field2:decimal,field3:bigint>"
        "struct<field1:string,field2:decimal(38, 9),field3:bigint>"   || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal( 38, 9),field3:bigint>"  || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal( 38 , 9),field3:bigint>" || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal(38,  9),field3:bigint>"  || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal(38,9),field3:bigint>"    || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal(38,9 ),field3:bigint>"   || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal (38,9 ),field3:bigint>"  || "struct<field1:string,field2:decimal(38,9),field3:bigint>"
        "struct<field1:string,field2:decimal (38),field3:bigint>"     || "struct<field1:string,field2:decimal(38),field3:bigint>"
        "struct<field1:string,field2:decimal(38 ),field3:bigint>"     || "struct<field1:string,field2:decimal(38),field3:bigint>"

        "struct<prediction_date:int,lower_confidence_amt:decimal(30, 2),upper_confidence_amt:decimal(30,2),model_short_name:string>" || "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>"
        "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30, 2),model_short_name:string>" || "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>"
        "struct<prediction_date:int,lower_confidence_amt:decimal(30, 2),upper_confidence_amt:decimal(30, 2),model_short_name:string>" || "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>"
        "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>" || "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>"

        "struct<prediction_cnt:int,first_prediction_date:int,last_prediction_date:int>" || "struct<prediction_cnt:int,first_prediction_date:int,last_prediction_date:int>"
        "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>" || "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2),model_short_name:string>"
        "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2)>" || "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),upper_confidence_amt:decimal(30,2)>"
        "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string,compression_factor:double>" || "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string,compression_factor:double>"
        "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string,pmvs_sticker_pts:double,pmvs_sticker_pts_lower_confidence_amt:double,pmvs_sticker_pts_upper_confidence_amt:double,pmvs_dt_pts:double,pmvs_dt_pct:double,pmvs_baseline_dt_pct:double,thumber_cnt:int,thumber_threshold_met:boolean,pmvs_dt_pts_ignoring_threshold:double,pmvs_dt_rmse:double,pmvs_baseline_dt_rmse:double>" || "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string,pmvs_sticker_pts:double,pmvs_sticker_pts_lower_confidence_amt:double,pmvs_sticker_pts_upper_confidence_amt:double,pmvs_dt_pts:double,pmvs_dt_pct:double,pmvs_baseline_dt_pct:double,thumber_cnt:int,thumber_threshold_met:boolean,pmvs_dt_pts_ignoring_threshold:double,pmvs_dt_rmse:double,pmvs_baseline_dt_rmse:double>"
        "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string>" || "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,model_short_name:string>"
        "struct<prediction_date:int,lower_confidence_amt:int,upper_confidence_amt:int,model_short_name:string>" || "struct<prediction_date:int,lower_confidence_amt:int,upper_confidence_amt:int,model_short_name:string>"
        "struct<prediction_date:int,prediction_source:string>" || "struct<prediction_date:int,prediction_source:string>"

        'array<struct<field2:decimal(38 ),countryCodes:array<string>,source:string>>' || 'array<struct<field2:decimal(38),countryCodes:array<string>,source:string>>'
        "struct<Field3:struct<Nested_Field1:bigint,Nested_Field2:bigint,Nested_FIELD3:decimal( 38, 9)>>" || "struct<Field3:struct<Nested_Field1:bigint,Nested_Field2:bigint,Nested_FIELD3:decimal(38,9)>>"
        "struct<Field1:decimal (38,9 ),Field2:bigint,field3:struct<NESTED_Field1:decimal ( 38,9 ),NesteD_Field2:bigint>>" || "struct<Field1:decimal(38,9),Field2:bigint,field3:struct<NESTED_Field1:decimal(38,9),NesteD_Field2:bigint>>"
    }

    @Unroll
    def 'fail to convert "#typeString" to a presto type and back'(String typeString) {
        when:
        converter.toMetacatType(typeString)
        then:
        thrown(Exception)
        where:
        typeString << [
            'list<string>',
            'struct<s: string>'
        ]
    }

    @Unroll
    def 'case reserve fieldName Fidelity'(String typeString, String expectedString) {
        expect:
        def result = converter.fromMetacatTypeToJson(converter.toMetacatType(typeString)).toString()

        assert result == expectedString

        where:
        typeString | expectedString
        "struct<Field1:bigint,Field2:bigint,field3:struct<nested_Field1:bigint,nested_Field2:bigint>>" | """{"type":"row","fields":[{"name":"Field1","type":"bigint"},{"name":"Field2","type":"bigint"},{"name":"field3","type":{"type":"row","fields":[{"name":"nested_Field1","type":"bigint"},{"name":"nested_Field2","type":"bigint"}]}}]}"""
        "array<struct<date:string,countryCodes:array<string>,source:string>>" | """{"type":"array","elementType":{"type":"row","fields":[{"name":"date","type":"string"},{"name":"countryCodes","type":{"type":"array","elementType":"string"}},{"name":"source","type":"string"}]}}"""
        "array<struct<Date:string,nestedArray:array<struct<date:string,countryCodes:array<string>,source:string>>>>" | """{"type":"array","elementType":{"type":"row","fields":[{"name":"Date","type":"string"},{"name":"nestedArray","type":{"type":"array","elementType":{"type":"row","fields":[{"name":"date","type":"string"},{"name":"countryCodes","type":{"type":"array","elementType":"string"}},{"name":"source","type":"string"}]}}}]}}"""
    }

    def "Test treat void transforms partitions as non-partition field"() {
        given:
        // Initial schema with three fields
        def initialSchema = new Schema(
            Types.NestedField.optional(1, "field1", Types.BooleanType.get(), "added 1st - partition key"),
            Types.NestedField.optional(2, "field2", Types.StringType.get(), "added 2nd"),
            Types.NestedField.optional(3, "field3", Types.IntegerType.get(), "added 3rd")
        )

        // Initial partition fields
        def initialPartitionFields = [
            new PartitionField(1, 1, "field1", new Identity()),
            new PartitionField(2, 2, "field2", new VoidTransform<String>()),
        ]

        when:
        def fieldDtos = this.converter.icebergSchemaTofieldDtos(initialSchema, initialPartitionFields)

        then:
        fieldDtos.size() == 3

        // Validate the first field
        def field1 = fieldDtos.find { it.name == "field1" }
        field1 != null
        field1.partitionKey == true

        // Validate the second field
        def field2 = fieldDtos.find { it.name == "field2" }
        field2 != null
        field2.partitionKey == false

        // Validate the third field
        def field3 = fieldDtos.find { it.name == "field3" }
        field3 != null
        field3.partitionKey == false

        noExceptionThrown()
    }
}
