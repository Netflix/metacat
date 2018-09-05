/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.common.server.usermetadata

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException
import com.netflix.metacat.common.server.properties.Config
import spock.lang.Shared
import spock.lang.Specification

class DefaultAuthorizationServiceSpec extends Specification{
    @Shared conf  = Mock(Config)

    def setupSpec() {
        Map<QualifiedName, Set<String>> map = new HashMap<>()
        map.put(QualifiedName.fromString("prodhive/abc"), ["bdp_janitor"].toSet())
        conf.getMetacatCreateAcl() >> { return map }
        conf.getMetacatDeleteAcl() >> { return map }
        conf.isAuthorizationEnabled() >> {return true}
    }

    def "Test unauthorized check"() {
        def authorizationService = new DefaultAuthorizationService(conf)
        when:
        authorizationService.checkPermission("",
            QualifiedName.fromString("prodhive/abcd") , MetacatOperation.CREATE)
        then:
        noExceptionThrown()
        when:
        authorizationService.checkPermission("bdp_janitor",
            QualifiedName.fromString("prodhive/abcd") , MetacatOperation.DELETE)
        then:
        noExceptionThrown()
        when:
        authorizationService.checkPermission("bdp_janitor",
            QualifiedName.fromString("prodhive/abcd") , MetacatOperation.RENAME)
        then:
        noExceptionThrown()

        when:
        authorizationService.checkPermission("bdp_janitor",
            QualifiedName.fromString("prodhive/abc") , MetacatOperation.CREATE)
        then:
        noExceptionThrown()

        when:
        authorizationService.checkPermission(null ,
            QualifiedName.fromString("prodhive/abc") , MetacatOperation.DELETE)
        then:
        thrown MetacatUnAuthorizedException

        when:
        authorizationService.checkPermission("bdp_janitor" ,
            QualifiedName.fromString("prodhive/abc") , MetacatOperation.DELETE)
        then:
        noExceptionThrown()

        when:
        authorizationService.checkPermission("bdp_janitor_2" ,
            QualifiedName.fromString("prodhive/abc") , MetacatOperation.DELETE)
        then:
        thrown MetacatUnAuthorizedException

        when:
        authorizationService.checkPermission("" ,
            QualifiedName.fromString("prodhive/abc") , MetacatOperation.DELETE)
        then:
        thrown MetacatUnAuthorizedException

        when:
        authorizationService.checkPermission("bdp_janitor_2",
            QualifiedName.fromString("prodhive/abc") , MetacatOperation.RENAME)
        then:
        thrown MetacatUnAuthorizedException
  }


}
