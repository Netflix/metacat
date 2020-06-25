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
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.usermetadata.AliasService
import com.netflix.spectator.api.Clock
import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Timer
import spock.lang.Specification

import java.util.function.Supplier

class RequestWrapperSpec extends Specification {

    def registry = Mock(Registry)
    def config = Mock(Config)
    def aliasService = Mock(AliasService)
    def rateLimiter = Mock(RateLimiter)
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
        requestWrapper = new RequestWrapper(registry, config, aliasService, rateLimiter)
    }

    def "Rate limiter is not invoked when disabled"() {
        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        1 * config.isRateLimiterEnabled() >> false
        0 * rateLimiter.hasExceededRequestLimit(_)
    }

    def "Rate limiter is not enforced when only enabled"() {
        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        1 * config.isRateLimiterEnabled() >> true
        1 * rateLimiter.hasExceededRequestLimit(_) >> true
        1 * config.isRateLimiterEnforced() >> false
        noExceptionThrown()
    }

    def "Exception is thrown when Rate limiter is enforced"() {
        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        1 * config.isRateLimiterEnabled() >> true
        1 * rateLimiter.hasExceededRequestLimit(_) >> true
        1 * config.isRateLimiterEnforced() >> true
        thrown(MetacatTooManyRequestsException)
    }
}
