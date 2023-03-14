/*
 *
 *  Copyright 2020 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.main.api

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatBadRequestException
import com.netflix.metacat.common.server.api.traffic_control.RequestGateway
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.usermetadata.AliasService
import com.netflix.metacat.common.server.util.MetacatContextManager
import com.netflix.spectator.api.*
import spock.lang.Specification

import java.util.function.Supplier

class RequestWrapperSpec extends Specification {

    def registry = Mock(Registry)
    def config = Mock(Config)
    def aliasService = Mock(AliasService)
    def requestGateway = Mock(RequestGateway)
    def timer = Mock(Timer)
    def clock = Mock(Clock)
    def counter = Mock(Counter)
    def id = Mock(Id)
    def supplier = Mock(Supplier)
    def requestWrapper

    def setup() {
        this.registry.clock() >> clock
        this.clock.wallTime() >> System.currentTimeMillis()
        this.registry.timer(_, _, _) >> this.timer
        this.registry.timer(_) >> this.timer
        this.registry.counter(_) >> counter
        this.registry.createId(_) >> id
        this.supplier.get() >> null
        requestWrapper = new RequestWrapper(registry, config, aliasService, requestGateway)
    }

    def "gateway is invoked for each request"() {
        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        1 * requestGateway.validateRequest("getTable", QualifiedName.fromString("a/b/c"))
    }

    def "throws the same exception from gateway"() {
        given:
        requestGateway.validateRequest("getTable", QualifiedName.fromString("a/b/c")) >> {
            throw new MetacatBadRequestException("asdf")
        }

        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        thrown(MetacatBadRequestException)
    }

    def "requestName is set in the context"() {
        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        MetacatContextManager.getContext().getRequestName() == "getTable"
    }
}
