package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException
import com.netflix.metacat.common.server.api.authorization.Authorization
import com.netflix.metacat.common.server.api.authorization.AuthorizationStatus
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiterRequestContext
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class ValidatingConnectorPartitionServiceSpec extends Specification {
    def delegate
    def rateLimiter
    def authorization
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
        authorization = Mock(Authorization)
        context = Mock(ConnectorRequestContext)

        name = QualifiedName.ofCatalog("c")
        newName = QualifiedName.ofCatalog("c2")
        resource = new PartitionInfo(name: name)
        tableInfo = new TableInfo(name: name)

        rateLimiterContext = new RateLimiterRequestContext("r1", name)

        MetacatContextManager.getContext().setRequestName("r1")
    }

    List<List> parametrizedCases() {
        return [
            [true, true, true, true],
            [true, true, true, false],
            [true, true, false, true],
            [true, true, false, false],
            [true, false, true, true],
            [true, false, true, false],
            [true, false, false, true],
            [true, false, false, false],
            [false, true, true, true],
            [false, true, true, false],
            [false, true, false, true],
            [false, true, false, false],
            [false, false, true, true],
            [false, false, true, false],
            [false, false, false, true],
            [false, false, false, false]
        ]
    }

    def "create"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.create(context, resource)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.create(context, resource)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "update"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.update(context, resource)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.update(context, resource)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "delete"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.delete(context, name)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.delete(context, name)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "get"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.get(context, name)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.get(context, name)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "list"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.list(context, name, null, null, null)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.list(context, name, null, null, null)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "listNames"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.listNames(context, name, null, null, null)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.listNames(context, name, null, null, null)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "rename"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.rename(context, name, newName)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.rename(context, name, newName)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "getPartitions"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.getPartitions(context, name, Mock(PartitionListRequest), tableInfo)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.getPartitions(context, name, _, tableInfo)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "getPartitionNames"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.getPartitionNames(context, [], true)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled || !rateLimiterEnabled) {
            if (authorizationEnabled) {
                1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
                if (!isAuthorized) {
                    0 * _
                }
            } else {
                1 * delegate.getPartitionNames(context, [], true)
            }
        }

        then:
        if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "savePartitions"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.savePartitions(context, name, Mock(PartitionsSaveRequest))
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.savePartitions(context, name, _)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "deletePartitions"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.deletePartitions(context, name, _, tableInfo)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.deletePartitions(context, name, _, tableInfo)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }

    def "getPartitionCount"() {
        setup:
        service = new ValidatingConnectorPartitionService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.getPartitionCount(context, name, tableInfo)
            throw new Success()
        }
        catch (Exception e) {
            actualException = e
        }

        then:
        if (rateLimiterEnabled) {
            1 * rateLimiter.hasExceededRequestLimit(rateLimiterContext) >> hasExceededLimit
            if (hasExceededLimit) {
                0 * _
            }
        }

        if (authorizationEnabled && (!rateLimiterEnabled || !hasExceededLimit)) {
            1 * authorization.isAuthorized(MetacatContextManager.getContext()) >> new AuthorizationStatus(isAuthorized, isAuthorized ? 'Authorized by default.' : 'Forbidden.')
            if (!isAuthorized) {
                0 * _
            }
        }

        if (!rateLimiterEnabled && !authorizationEnabled || !hasExceededLimit && isAuthorized) {
            1 * delegate.getPartitionCount(context, name, tableInfo)
        }

        then:
        if (rateLimiterEnabled && hasExceededLimit) {
            assert actualException instanceof MetacatTooManyRequestsException
        } else if (authorizationEnabled && !isAuthorized) {
            assert actualException instanceof MetacatUnAuthorizedException
        } else {
            assert actualException instanceof Success
        }

        where:
        [rateLimiterEnabled, hasExceededLimit, authorizationEnabled, isAuthorized] << parametrizedCases()
    }
}
