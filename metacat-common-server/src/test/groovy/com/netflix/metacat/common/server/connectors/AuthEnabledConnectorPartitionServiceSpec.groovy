package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException
import com.netflix.metacat.common.server.api.authorization.Authorization
import com.netflix.metacat.common.server.api.authorization.AuthorizationStatus
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class AuthEnabledConnectorPartitionServiceSpec extends Specification {
    def delegate
    def authorization
    def context

    def resource
    def tableInfo
    def name
    def newName
    def service

    class Success extends RuntimeException {}

    def setup() {
        delegate = Mock(ConnectorPartitionService)
        authorization = Mock(Authorization)
        context = Mock(ConnectorRequestContext)

        name = QualifiedName.ofCatalog("c")
        newName = QualifiedName.ofCatalog("c2")
        resource = new PartitionInfo(name: name)
        tableInfo = new TableInfo(name: name)

        service = new AuthEnabledConnectorPartitionService(delegate, authorization)
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

    def "getPartitions"() {
        when:
        service.getPartitions(context, name, Mock(PartitionListRequest), tableInfo)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.getPartitions(context, name, _, tableInfo)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "savePartitions"() {
        when:
        service.savePartitions(context, name, Mock(PartitionsSaveRequest))
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.savePartitions(context, name, _,)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "deletePartitions"() {
        when:
        service.deletePartitions(context, name, _, tableInfo)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.deletePartitions(context, name, _, tableInfo)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }

    def "getPartitionCount"() {
        when:
        service.getPartitionCount(context, name, tableInfo)
        throw new Success()

        then:
        thrown(expectedException)
        authorization.isAuthorized(MetacatContextManager.getContext()) >> authStatus

        if (authStatus.isAuthorized()) {
            1 * delegate.getPartitionCount(context, name, tableInfo)
        }

        where:
        expectedException            | authStatus
        Success                      | new AuthorizationStatus(true, "Authorized by default.")
        MetacatUnAuthorizedException | new AuthorizationStatus(false, "Forbidden.")
    }
}
