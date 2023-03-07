package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiterRequestContext
import com.netflix.metacat.common.server.connectors.model.CatalogInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class ThrottlingConnectorCatalogServiceSpec extends Specification {
    def delegate
    def rateLimiter
    def context

    def resource
    def name
    def newName
    def rateLimiterContext
    def service

    class Success extends RuntimeException {}

    def setup() {
        delegate = Mock(ConnectorCatalogService)
        rateLimiter = Mock(RateLimiter)
        context = Mock(ConnectorRequestContext)

        name = QualifiedName.ofCatalog("c")
        newName = QualifiedName.ofCatalog("c2")
        resource = new CatalogInfo(name: name)

        rateLimiterContext = new RateLimiterRequestContext("r1", name)

        service = new ThrottlingConnectorCatalogService(delegate, rateLimiter)

        MetacatContextManager.getContext().setRequestName("r1")
    }

    def "create"() {
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
}
