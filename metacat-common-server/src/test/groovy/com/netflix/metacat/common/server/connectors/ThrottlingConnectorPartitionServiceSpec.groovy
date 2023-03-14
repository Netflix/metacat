package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiterRequestContext
import com.netflix.metacat.common.server.connectors.model.CatalogInfo
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class ThrottlingConnectorPartitionServiceSpec extends Specification {
    def delegate
    def rateLimiter
    def context

    def resource
    def tableInfo
    def name
    def newName
    def rateLimiterContext
    def service

    class Success extends RuntimeException {}

    def setup() {
        delegate = Mock(ConnectorPartitionService)
        rateLimiter = Mock(RateLimiter)
        context = Mock(ConnectorRequestContext)

        name = QualifiedName.ofCatalog("c")
        newName = QualifiedName.ofCatalog("c2")
        resource = new PartitionInfo(name: name)
        tableInfo = new TableInfo(name: name)

        rateLimiterContext = new RateLimiterRequestContext("r1", name)

        service = new ThrottlingConnectorPartitionService(delegate, rateLimiter)
    }

    def "create"() {
        given:
        MetacatContextManager.getContext().setRequestName("r1")

        when:
        service.create(context, resource)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.create(context, resource)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "update"() {

        when:
        service.update(context, resource)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.update(context, resource)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "delete"() {
        when:
        service.delete(context, name)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.delete(context, name)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "get"() {
        when:
        service.get(context, name)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.get(context, name)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "exists"() {
        when:
        service.exists(context, name)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.exists(context, name)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "list"() {
        when:
        service.list(context, name, null, null, null)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.list(context, name, null, null, null)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "listNames"() {
        when:
        service.listNames(context, name, null, null, null)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.listNames(context, name, null, null, null)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "rename"() {
        when:
        service.rename(context, name, newName)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.rename(context, name, newName)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "getPartitions"() {
        when:
        service.getPartitions(context, name, Mock(PartitionListRequest), tableInfo)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.getPartitions(context, name, _, tableInfo)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "savePartitions"() {
        when:
        service.savePartitions(context, name, Mock(PartitionsSaveRequest))
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.savePartitions(context, name, _,)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "deletePartitions"() {
        when:
        service.deletePartitions(context, name, _, tableInfo)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.deletePartitions(context, name, _, tableInfo)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }

    def "getPartitionCount"() {
        when:
        service.getPartitionCount(context, name, tableInfo)
        throw new Success()

        then:
        thrown(expectedException)
        rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> exceeded

        if (!exceeded) {
            1 * delegate.getPartitionCount(context, name, tableInfo)
        }

        where:
        exceeded | expectedException
        true     | MetacatTooManyRequestsException
        false    | Success
    }
}
