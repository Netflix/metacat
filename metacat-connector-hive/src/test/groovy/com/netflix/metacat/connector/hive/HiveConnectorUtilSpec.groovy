/*
 *  Copyright 2017 Netflix, Inc.
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
 */

package com.netflix.metacat.connector.hive

import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.connectors.util.TimeUtil
import com.netflix.metacat.common.server.util.MetacatUtils
import com.netflix.metacat.connector.hive.util.HiveTableUtil
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

/**
 * HiveConnectorUtilSpec.
 * @author zhenl
 * @since 1.0.0
 */
class HiveConnectorUtilSpec extends Specification{
    @Unroll
    def "Test for get socket timeout" (){
        given:
        def timeout = TimeUtil.toTime(inputString, TimeUnit.SECONDS, TimeUnit.SECONDS)
        def timeoutm = TimeUtil.toTime(inputString, TimeUnit.SECONDS, TimeUnit.MILLISECONDS)
        expect:
        timeout == output
        timeoutm == output*1000
        where:
        inputString | output
        '2000s'     | 2000L
        '1'         | 1L
        '010m'      | 600L
    }

    def "Test for check common view" () {
        given:
        def tableInfo = TableInfo.builder().metadata(metadata).build()
        def isView = HiveTableUtil.isCommonView(tableInfo)
        def isView2 = MetacatUtils.isCommonView(tableInfo.metadata)
        expect:
        isView == output
        isView2 == output
        where:
        metadata     | output
        ['common_view' : "true"]    | true
        ['common_view' : "false"]    | false
        ['common_view_1' : "false"]    | false
        ['common_view' : null]    | false
        ['common_view' : "True"]    | true
        ['common_view' : ""]    | false
        [:]    | false
        null | false
      }
}
