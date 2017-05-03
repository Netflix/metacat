package com.netflix.metacat.common.server.converter

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.server.properties.ArchaiusConfigImpl
import spock.lang.Shared
import spock.lang.Specification

/**
 * ConverterUtil tests.
 *
 * @author amajumdar
 * @since 1.0.0
 */
class ConverterUtilSpec extends Specification {
    @Shared
    def converter = new ConverterUtil(new DozerTypeConverter(
        new TypeConverterFactory(new ArchaiusConfigImpl())))

    def testDatabaseConversion() {
        given:
        def dto = new DatabaseDto(name: QualifiedName.ofDatabase('prodhive', 'amajumdar'),
            metadata: ['a': 'b'])
        when:
        def info = converter.fromDatabaseDto(dto)
        def resultDto = converter.toDatabaseDto(info)
        then:
        dto == resultDto
        when:
        dto.setTables(['part'])
        info = converter.fromDatabaseDto(dto)
        resultDto = converter.toDatabaseDto(info)
        then:
        dto != resultDto
    }

    def testTableConversion() {
        given:
        def dto = new TableDto(name: QualifiedName.ofTable('prodhive', 'amajumdar', 'part'),
            audit: new AuditDto('test', new Date(), 'test', new Date()),
            fields: [new FieldDto(null, 'esn', true, 0, 'string', 'string', false, null, null, false, false)],
            serde: new StorageDto(owner: 'test'))
        when:
        def info = converter.fromTableDto(dto)
        def resultDto = converter.toTableDto(info)
        then:
        dto == resultDto
    }

    def testPartitionConversion() {
        given:
        def dto = new PartitionDto(name: QualifiedName.ofPartition('prodhive', 'amajumdar', 'part', 'dateint=20170101/h=1'),
            audit: new AuditDto('test', new Date(), 'test', new Date()),
            serde: new StorageDto(owner: 'test'))
        when:
        def info = converter.fromPartitionDto(dto)
        def resultDto = converter.toPartitionDto(info)
        then:
        dto == resultDto
    }
}
