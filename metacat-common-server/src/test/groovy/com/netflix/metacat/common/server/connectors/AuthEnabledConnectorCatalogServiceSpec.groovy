package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException
import com.netflix.metacat.common.server.api.authorization.Authorization
import com.netflix.metacat.common.server.api.authorization.AuthorizationStatus
import com.netflix.metacat.common.server.connectors.model.CatalogInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class AuthEnabledConnectorCatalogServiceSpec extends Specification {
    def delegate
    def authorization
    def context

    def resource
    def name
    def newName
    def service

    class Success extends RuntimeException {}

    def setup() {
        delegate = Mock(ConnectorCatalogService)
        authorization = Mock(Authorization)
        context = Mock(ConnectorRequestContext)

        name = QualifiedName.ofCatalog("c")
        newName = QualifiedName.ofCatalog("c2")
        resource = new CatalogInfo(name: name)

        service = new AuthEnabledConnectorCatalogService(delegate, authorization)

        MetacatContextManager.getContext().setRequestName("r1")
    }

    def "create"() {
        when:
        service.create(context, resource)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.create(context, resource)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "update"() {

        when:
        service.update(context, resource)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.update(context, resource)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "delete"() {
        when:
        service.delete(context, name)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.delete(context, name)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "get"() {
        when:
        service.get(context, name)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.get(context, name)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "exists"() {
        when:
        service.exists(context, name)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.exists(context, name)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "list"() {
        when:
        service.list(context, name, null, null, null)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.list(context, name, null, null, null)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "listNames"() {
        when:
        service.listNames(context, name, null, null, null)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.listNames(context, name, null, null, null)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "rename"() {
        when:
        service.rename(context, name, newName)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.rename(context, name, newName)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }
}
