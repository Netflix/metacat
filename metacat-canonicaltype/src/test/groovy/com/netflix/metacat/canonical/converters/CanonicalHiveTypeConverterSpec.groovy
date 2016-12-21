package com.netflix.metacat.canonical.converters

import com.netflix.metacat.canonical.type.TypeManager
import com.netflix.metacat.canonical.type.TypeRegistry
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Created by zhenli on 12/20/16.
 */
class CanonicalHiveTypeConverterSpec extends Specification {
    @Shared
    CanonicalHiveTypeConverter converter = new CanonicalHiveTypeConverter()
    @Shared
    TypeManager typeManager = new TypeRegistry()

    @Unroll
    def 'can convert "#typeString" to a presto type and back'(String typeString) {
        expect:
        def prestoType = converter.dataTypeToCanonicalType(typeString, typeManager)
        def hiveType = converter.canonicalTypeToDataType(prestoType)
        def prestoTypeFromHiveType = converter.dataTypeToCanonicalType(hiveType, typeManager)
        prestoTypeFromHiveType == prestoType
        where:
        typeString << [
//            'tinyint',
//            'smallint',
//            'boolean',
//            'decimal',
//            'decimal(5,2)',
            'char(10)',
            'map<boolean,boolean>',
//            'map<boolean,string>',
//            'map<bigint,bigint>',
//            'map<string,double>',
//            'map<string,bigint>',
//            'map<string,string>',
//            'map<string,struct<field1:array<bigint>>>'
        ]
    }

}
