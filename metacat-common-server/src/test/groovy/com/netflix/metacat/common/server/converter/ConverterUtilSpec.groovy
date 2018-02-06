package com.netflix.metacat.common.server.converter

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
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
    def converter = new ConverterUtil(
        new DozerTypeConverter(
            new TypeConverterFactory(
                new DefaultConfigImpl(
                    new MetacatProperties()
                )
            )
        )
    )

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
            fields: [new FieldDto(null, 'esn', true, 0, 'string', 'string', false, null, null, false, false)],
            serde: new StorageDto(owner: 'test'))
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
            fields: [new FieldDto(null, 'esn', true, 0, 'string', 'string', false, null, null, false, false)],
            serde: new StorageDto(owner: 'test'),
            view: new ViewDto("select test", "select test2"))
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
