/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The expected strings are taken verbatim from Metacat's own {@code HiveTypeConverterSpec}, which
 * asserts they survive its Hive round trip unchanged. A failure here means this module and the
 * Hive connector would render the same column differently.
 */
class IcebergTypeConverterTest {

  private static final AtomicInteger IDS = new AtomicInteger();

  private static final Type BOOLEAN = Types.BooleanType.get();
  private static final Type INT = Types.IntegerType.get();
  private static final Type BIGINT = Types.LongType.get();
  private static final Type DOUBLE = Types.DoubleType.get();
  private static final Type STRING = Types.StringType.get();

  private static Types.NestedField f(String name, Type type) {
    return Types.NestedField.optional(IDS.incrementAndGet(), name, type);
  }

  private static Type struct(Types.NestedField... fields) {
    return Types.StructType.of(fields);
  }

  private static Type array(Type element) {
    return Types.ListType.ofOptional(IDS.incrementAndGet(), element);
  }

  private static Type map(Type key, Type value) {
    return Types.MapType.ofOptional(IDS.incrementAndGet(), IDS.incrementAndGet(), key, value);
  }

  private static Type decimal(int precision, int scale) {
    return Types.DecimalType.of(precision, scale);
  }

  /** Every case from Metacat's corpus that an Iceberg schema can actually express. */
  static Stream<Arguments> metacatCorpus() {
    return Stream.of(
        arguments(BOOLEAN, "boolean"),
        arguments(INT, "int"),
        arguments(BIGINT, "bigint"),
        arguments(Types.FloatType.get(), "float"),
        arguments(DOUBLE, "double"),
        arguments(Types.DateType.get(), "date"),
        arguments(Types.TimestampType.withoutZone(), "timestamp"),
        arguments(Types.TimestampType.withZone(), "timestamp"),
        arguments(STRING, "string"),
        arguments(Types.UUIDType.get(), "string"),
        arguments(Types.BinaryType.get(), "binary"),
        arguments(Types.FixedType.ofLength(16), "binary"),
        arguments(decimal(4, 2), "decimal(4,2)"),
        arguments(decimal(38, 9), "decimal(38,9)"),
        arguments(array(decimal(4, 2)), "array<decimal(4,2)>"),
        arguments(array(BIGINT), "array<bigint>"),
        arguments(array(BOOLEAN), "array<boolean>"),
        arguments(array(DOUBLE), "array<double>"),
        arguments(array(STRING), "array<string>"),
        arguments(array(map(BIGINT, BIGINT)), "array<map<bigint,bigint>>"),
        arguments(array(map(BIGINT, STRING)), "array<map<bigint,string>>"),
        arguments(array(map(STRING, BIGINT)), "array<map<string,bigint>>"),
        arguments(array(map(STRING, STRING)), "array<map<string,string>>"),
        arguments(
            array(struct(f("field1", BIGINT), f("field2", BIGINT))),
            "array<struct<field1:bigint,field2:bigint>>"),
        arguments(
            array(struct(f("field1", BIGINT), f("field2", STRING))),
            "array<struct<field1:bigint,field2:string>>"),
        arguments(
            array(struct(f("field1", STRING), f("field2", BIGINT))),
            "array<struct<field1:string,field2:bigint>>"),
        arguments(
            array(struct(f("field1", STRING), f("field2", STRING))),
            "array<struct<field1:string,field2:string>>"),
        arguments(map(BOOLEAN, BOOLEAN), "map<boolean,boolean>"),
        arguments(map(BOOLEAN, STRING), "map<boolean,string>"),
        arguments(map(BIGINT, BIGINT), "map<bigint,bigint>"),
        arguments(map(STRING, DOUBLE), "map<string,double>"),
        arguments(map(STRING, BIGINT), "map<string,bigint>"),
        arguments(map(STRING, STRING), "map<string,string>"),
        arguments(
            map(STRING, struct(f("field1", array(BIGINT)))),
            "map<string,struct<field1:array<bigint>>>"),
        arguments(
            struct(f("field1", BIGINT), f("field2", BIGINT), f("field3", BIGINT)),
            "struct<field1:bigint,field2:bigint,field3:bigint>"),
        arguments(
            struct(f("field1", BIGINT), f("field2", STRING), f("field3", DOUBLE)),
            "struct<field1:bigint,field2:string,field3:double>"),
        arguments(
            struct(f("field1", STRING), f("field2", decimal(38, 9)), f("field3", BIGINT)),
            "struct<field1:string,field2:decimal(38,9),field3:bigint>"),
        // Production shapes lifted from the same corpus
        arguments(
            struct(
                f("prediction_date", INT),
                f("lower_confidence_amt", decimal(30, 2)),
                f("upper_confidence_amt", decimal(30, 2)),
                f("model_short_name", STRING)),
            "struct<prediction_date:int,lower_confidence_amt:decimal(30,2),"
                + "upper_confidence_amt:decimal(30,2),model_short_name:string>"),
        arguments(
            struct(
                f("prediction_cnt", INT),
                f("first_prediction_date", INT),
                f("last_prediction_date", INT)),
            "struct<prediction_cnt:int,first_prediction_date:int,last_prediction_date:int>"),
        arguments(
            struct(
                f("prediction_date", INT),
                f("lower_confidence_amt", DOUBLE),
                f("upper_confidence_amt", DOUBLE),
                f("model_short_name", STRING),
                f("pmvs_sticker_pts", DOUBLE),
                f("pmvs_sticker_pts_lower_confidence_amt", DOUBLE),
                f("pmvs_sticker_pts_upper_confidence_amt", DOUBLE),
                f("pmvs_dt_pts", DOUBLE),
                f("pmvs_dt_pct", DOUBLE),
                f("pmvs_baseline_dt_pct", DOUBLE),
                f("thumber_cnt", INT),
                f("thumber_threshold_met", BOOLEAN),
                f("pmvs_dt_pts_ignoring_threshold", DOUBLE),
                f("pmvs_dt_rmse", DOUBLE),
                f("pmvs_baseline_dt_rmse", DOUBLE)),
            "struct<prediction_date:int,lower_confidence_amt:double,upper_confidence_amt:double,"
                + "model_short_name:string,pmvs_sticker_pts:double,"
                + "pmvs_sticker_pts_lower_confidence_amt:double,"
                + "pmvs_sticker_pts_upper_confidence_amt:double,pmvs_dt_pts:double,"
                + "pmvs_dt_pct:double,pmvs_baseline_dt_pct:double,thumber_cnt:int,"
                + "thumber_threshold_met:boolean,pmvs_dt_pts_ignoring_threshold:double,"
                + "pmvs_dt_rmse:double,pmvs_baseline_dt_rmse:double>"),
        // Metacat's "Nested Type with UpperCase" cases
        arguments(
            array(struct(f("date", STRING), f("countryCodes", array(STRING)), f("source", STRING))),
            "array<struct<date:string,countryCodes:array<string>,source:string>>"),
        arguments(
            struct(f("Field3", struct(f("Nested_Field1", BIGINT), f("Nested_Field2", BIGINT)))),
            "struct<Field3:struct<Nested_Field1:bigint,Nested_Field2:bigint>>"),
        arguments(
            struct(
                f("Field1", BIGINT),
                f("Field2", BIGINT),
                f("field3", struct(f("NESTED_Field1", BIGINT), f("NesteD_Field2", BIGINT)))),
            "struct<Field1:bigint,Field2:bigint,"
                + "field3:struct<NESTED_Field1:bigint,NesteD_Field2:bigint>>"));
  }

  @ParameterizedTest
  @MethodSource("metacatCorpus")
  void rendersTheSameTypeStringAsMetacat(Type type, String expected) {
    assertThat(IcebergTypeConverter.toTypeString(type)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({"38,9", "10,0", "1,1", "30,2", "4,2"})
  void decimalCarriesPrecisionAndScaleWithoutSpaces(int precision, int scale) {
    Type type = decimal(precision, scale);

    assertThat(IcebergTypeConverter.toTypeString(type))
        .isEqualTo(String.format("decimal(%s,%s)", precision, scale));
    assertThat(IcebergTypeConverter.toJsonType(type).toString())
        .isEqualTo(
            String.format(
                "{\"type\":\"decimal\",\"precision\":%s,\"scale\":%s}", precision, scale));
  }

  @Test
  void nonParametricTypesRenderAsAJsonString() {
    assertThat(IcebergTypeConverter.toJsonType(BIGINT).toString()).isEqualTo("\"bigint\"");
    assertThat(IcebergTypeConverter.toJsonType(STRING).toString()).isEqualTo("\"string\"");
    assertThat(IcebergTypeConverter.toJsonType(Types.UUIDType.get()).toString())
        .isEqualTo("\"string\"");
  }

  @Test
  void binaryRendersAsAnObjectBecauseVarbinaryIsParametricInMetacat() {
    assertThat(IcebergTypeConverter.toJsonType(Types.BinaryType.get()).toString())
        .isEqualTo("{\"type\":\"binary\"}");
    assertThat(IcebergTypeConverter.toJsonType(Types.FixedType.ofLength(16)).toString())
        .isEqualTo("{\"type\":\"binary\"}");
  }

  /** Expected JSON taken from Metacat's "case reserve fieldName Fidelity" cases. */
  @Test
  void structJsonPreservesFieldNameCase() {
    Type type =
        struct(
            f("Field1", BIGINT),
            f("Field2", BIGINT),
            f("field3", struct(f("nested_Field1", BIGINT), f("nested_Field2", BIGINT))));

    assertThat(IcebergTypeConverter.toJsonType(type).toString())
        .isEqualTo(
            "{\"type\":\"row\",\"fields\":[{\"name\":\"Field1\",\"type\":\"bigint\"},"
                    + "{\"name\":\"Field2\",\"type\":\"bigint\"},{\"name\":\"field3\",\"type\":{\"type\":\"row\","
                    + "\"fields\":[{\"name\":\"nested_Field1\",\"type\":\"bigint\"},{\"name\":\"nested_Field2\","
                    + "\"type\":\"bigint\"}]}}]}");
  }

  @Test
  void listOfStructWithNestedListJson() {
    Type type =
        array(struct(f("date", STRING), f("countryCodes", array(STRING)), f("source", STRING)));

    assertThat(IcebergTypeConverter.toJsonType(type).toString())
        .isEqualTo(
            "{\"type\":\"array\",\"elementType\":{\"type\":\"row\",\"fields\":[{\"name\":\"date\","
                    + "\"type\":\"string\"},{\"name\":\"countryCodes\",\"type\":{\"type\":\"array\","
                    + "\"elementType\":\"string\"}},{\"name\":\"source\",\"type\":\"string\"}]}}");
  }

  @Test
  void mapJson() {
    assertThat(IcebergTypeConverter.toJsonType(map(STRING, decimal(38, 9))).toString())
        .isEqualTo(
            "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"decimal\","
                    + "\"precision\":38,\"scale\":9}}");
  }

  @Test
  void timeIsRejectedTheWayMetacatRejectsIt() {
    assertThatThrownBy(() -> IcebergTypeConverter.toTypeString(Types.TimeType.get()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Hive does not support time fields");
  }
}
