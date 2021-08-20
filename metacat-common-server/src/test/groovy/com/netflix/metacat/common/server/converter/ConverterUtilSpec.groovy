package com.netflix.metacat.common.server.converter

import com.fasterxml.jackson.databind.node.TextNode
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import org.dozer.MappingException
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
    def typeFactory = new TypeConverterFactory(new DefaultTypeConverter())
    @Shared
    def converter = new ConverterUtil(new DozerTypeConverter(typeFactory), new DozerJsonTypeConverter(typeFactory))

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

        when:
        converter.fromDatabaseDto(null)
        then:
        thrown MappingException

        when:
        converter.toDatabaseDto(null)
        then:
        thrown MappingException
    }

    def testTableConversion() {
        given:
        def dto = new TableDto(name: QualifiedName.ofTable('prodhive', 'amajumdar', 'part'),
            audit: new AuditDto('test', new Date(), 'test', new Date()),
            fields: [FieldDto.builder().name('esn').type('string').source_type('string')
                         .jsonType(new TextNode('string')).pos(0).build()] ,
            serde: new StorageDto(owner: 'test'),
            keys: KeySetDto.builder()
                .partition(Arrays.asList(KeyDto.builder().name('partition').fields(Arrays.asList('esn')).build()))
                .build()
        )
        when:
        def info = converter.fromTableDto(dto)
        def resultDto = converter.toTableDto(info)
        then:
        dto == resultDto
    }

    def testTableViewConversion() {
        given:
        def dto = new TableDto(name: QualifiedName.ofTable('prodhive', 'amajumdar', 'part'),
            audit: new AuditDto('test', new Date(), 'test', new Date()),
            fields: [FieldDto.builder().name('esn').type('string').source_type('string').jsonType(new TextNode('string')).pos(0).build()],
            serde: new StorageDto(owner: 'test'),
            view: new ViewDto("select test", "select test2"),
            keys: KeySetDto.builder()
                .partition(Arrays.asList(KeyDto.builder().name('partition').fields(Arrays.asList('esn')).build()))
                .build()
        )
        when:
        def info = converter.fromTableDto(dto)
        def resultDto = converter.toTableDto(info)
        then:
        dto == resultDto
        when:
        converter.fromTableDto(null)
        then:
        thrown MappingException

        when:
        converter.toTableDto(null)
        then:
        thrown MappingException
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
        when:
        converter.fromPartitionDto(null)
        then:
        thrown MappingException
        when:
        converter.toPartitionDto(null)
        then:
        thrown MappingException
    }

    def testPartitionListRequestConversion() {
        given:
        def dto = new GetPartitionsRequestDto(null, null, false, false);
        def sort = new Sort()
        def pagable = new Pageable()
        when:
        def req = converter.toPartitionListRequest(dto, pagable, sort)
        then:
        req.getFilter() == dto.getFilter()
        req.getPartitionNames() == dto.getPartitionNames()
        req.getIncludeAuditOnly() == dto.getIncludeAuditOnly()
        req.getIncludePartitionDetails() == dto.getIncludePartitionDetails()

        when:
        req = converter.toPartitionListRequest(null, pagable, sort)
        then:
        req.getFilter() == dto.getFilter()
        req.getPartitionNames() == dto.getPartitionNames()
        req.getIncludeAuditOnly() == dto.getIncludeAuditOnly()
        req.getIncludePartitionDetails() == dto.getIncludePartitionDetails()

        when:
        req = converter.toPartitionListRequest(new GetPartitionsRequestDto(), pagable, sort)
        then:
        req.getFilter() == dto.getFilter()
        req.getPartitionNames() == dto.getPartitionNames()
        req.getIncludeAuditOnly() == dto.getIncludeAuditOnly()
        req.getIncludePartitionDetails() == dto.getIncludePartitionDetails()

    }
}
