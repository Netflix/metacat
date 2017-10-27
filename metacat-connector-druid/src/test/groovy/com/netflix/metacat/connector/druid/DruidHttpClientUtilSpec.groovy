package com.netflix.metacat.connector.druid

import com.netflix.metacat.connector.druid.client.DruidHttpClientUtil
import spock.lang.Specification

/**
 * DruidHttpClientUtilSpec.
 * @author zhenl
 * @since 1.2.0
 */
class DruidHttpClientUtilSpec extends Specification{
    
    def "Test for getLatestDataByName"() {

        when:
        def ret = DruidHttpClientUtil.getLatestSegment(getInput())

        then:
        ret.equals("algodash_map_row_report_agg_2016-09-03T00:00:00.000Z_2016-09-04T00:00:00.000Z_2016-10-10T21:19:50.893Z");

    }

    private static String getInput() {
        return "[\n" +
            "\"algodash_map_row_report_agg_2016-09-01T00:00:00.000Z_2016-09-02T00:00:00.000Z_2016-10-10T21:19:50.893Z\",\n" +
            "\"algodash_map_row_report_agg_2016-09-02T00:00:00.000Z_2016-09-03T00:00:00.000Z_2016-10-10T21:19:50.893Z\",\n" +
            "\"algodash_map_row_report_agg_2016-09-03T00:00:00.000Z_2016-09-04T00:00:00.000Z_2016-10-10T21:19:50.893Z\"\n" +
            "]";
    }
}
