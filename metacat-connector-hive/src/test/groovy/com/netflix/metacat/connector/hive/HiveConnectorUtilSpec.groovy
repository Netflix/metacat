package com.netflix.metacat.connector.hive

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
    def "Test for get socke timeout" (){
        given:
        def timeout = HiveConnectorUtil.toTime(inputString, TimeUnit.SECONDS, TimeUnit.SECONDS)
        def timeoutm = HiveConnectorUtil.toTime(inputString, TimeUnit.SECONDS, TimeUnit.MILLISECONDS)
        expect:
        timeout == output
        timeoutm == output*1000
        where:
        inputString | output
        '2000s'     | 2000L
        '1'         | 1L
        '010m'      | 600L
    }
}
