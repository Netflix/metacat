package com.netflix.metacat.main.services.impl

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.TableDto
import com.netflix.spectator.api.DefaultRegistry
import spock.lang.Specification
import spock.lang.Unroll

class DefaultOwnerValidationServiceSpec extends Specification {
    def objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setSerializationInclusion(JsonInclude.Include.ALWAYS)

    def registry
    def service

    def setup() {
        registry = new DefaultRegistry()
        service = new DefaultOwnerValidationService(registry)
    }

    @Unroll
    def "test for known invalid owners"() {
        def name = QualifiedName.fromString("c/d/t")
        def definitionMetadataJson = toObjectNode(definitionMetadata)
        def tableDto = new TableDto(
            name: name,
            definitionMetadata: definitionMetadataJson
        )

        when:
        service.enforceOwnerValidation("op", name, tableDto)

        then:
        noExceptionThrown()
        registry.counter(
            "metacat.table.owner.invalid",
            "operation", "op",
            "catalogAndDb", "c_d"
        ).count() == metricCount

        where:
        definitionMetadata                                      || metricCount
        null                                                    || 1
        "{}"                                                    || 1
        "{\"owner\":null}"                                      || 1
        "{\"owner\":{}}"                                        || 1
        "{\"owner\":{\"userId\":null}}"                         || 1
        "{\"owner\":{\"userId\":\"\"}}"                         || 1
        "{\"owner\":{\"userId\":\" \"}}"                        || 1
        "{\"owner\":{\"userId\":\"metacat\"}}"                  || 1
        "{\"owner\":{\"userId\":\"root\"}}"                     || 1
        "{\"owner\":{\"userId\":\"metacat-thrift-interface\"}}" || 1
        "{\"owner\":{\"userId\":\"ssarma\"}}"                   || 0
        "{\"owner\":{\"userId\":\"ssarma\"}}"                   || 0
    }

    ObjectNode toObjectNode(jsonString) {
        return jsonString == null ? null : (objectMapper.readTree(jsonString as String) as ObjectNode)
    }
}
