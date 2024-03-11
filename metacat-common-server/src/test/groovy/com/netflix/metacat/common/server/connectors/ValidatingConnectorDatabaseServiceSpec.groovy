package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException
import com.netflix.metacat.common.server.api.authorization.Authorization
import com.netflix.metacat.common.server.api.authorization.AuthorizationStatus
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiterRequestContext
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class ValidatingConnectorDatabaseServiceSpec extends Specification {
    def delegate
    def rateLimiter
    def authorization
    def context

    def resource
    def name
    def newName
    def rateLimiterContext
    def service

    class Success extends RuntimeException {}

    def setup() {
        delegate = Mock(ConnectorDatabaseService)
        rateLimiter = Mock(RateLimiter)
        authorization = Mock(Authorization)
        context = Mock(ConnectorRequestContext)

        name = QualifiedName.ofDatabase("c", "d")
        newName = QualifiedName.ofDatabase("c2", "d2")
        resource = new DatabaseInfo(name: name)

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
        service = new ValidatingConnectorDatabaseService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

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
        service = new ValidatingConnectorDatabaseService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

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

    def "get"() {
        setup:
        service = new ValidatingConnectorDatabaseService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

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

    def "exists"() {
        setup:
        service = new ValidatingConnectorDatabaseService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.exists(context, name)
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
            1 * delegate.exists(context, name)
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
        service = new ValidatingConnectorDatabaseService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

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
        service = new ValidatingConnectorDatabaseService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

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
        service = new ValidatingConnectorDatabaseService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

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

    def "listViewNames"() {
        setup:
        service = new ValidatingConnectorDatabaseService(delegate, rateLimiter, rateLimiterEnabled, authorization, authorizationEnabled)

        when:
        def actualException = null
        try {
            service.listViewNames(context, name)
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
            1 * delegate.listViewNames(context, name)
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
