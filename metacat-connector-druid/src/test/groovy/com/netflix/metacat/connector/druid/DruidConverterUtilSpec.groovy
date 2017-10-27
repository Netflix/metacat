package com.netflix.metacat.connector.druid

import com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.connector.druid.converter.DruidConverterUtil
import spock.lang.Specification

/**
 * DruidConverterUtil Test.
 */
class DruidConverterUtilSpec extends Specification{
    static final MetacatJson metacatJson = new MetacatJsonLocator()

    def "Test for getDatasourceFromLatestSegmentJsonobject" () {
        ObjectNode node = metacatJson.parseJsonObject(getJsonDatasource())
        when:
        def ret = DruidConverterUtil.getDatasourceFromAllSegmentJsonObject(node)
        then:
        assert ret.name.equals("algodash_map_row_report_agg")
        assert ret.segmentList.size() == 2
        assert ret.segmentList.get(0).name.equals("algodash_map_row_report_agg")
        assert ret.segmentList.get(0).loadSpec.uri.equals("netflix-dataoven-prod-users/druid/bdp_druid_prod/algodash_map_row_report_agg/2016-09-01T00:00:00.000Z_2016-09-02T00:00:00.000Z/2016-10-10T21:19:50.893Z/0")
        assert ret.segmentList.get(0).loadSpec.keys.equals(["druid/bdp_druid_prod/algodash_map_row_report_agg/2016-09-01T00:00:00.000Z_2016-09-02T00:00:00.000Z/2016-10-10T21:19:50.893Z/0/index.zip"])
        assert ret.segmentList.get(1).name.equals("algodash_map_row_report_agg")
        assert ret.segmentList.get(1).loadSpec.uri.equals("netflix-dataoven-prod-users/druid/bdp_druid_prod/algodash_map_row_report_agg/2016-09-02T00:00:00.000Z_2016-09-03T00:00:00.000Z/2016-10-10T21:19:50.893Z/0")
        assert ret.segmentList.get(1).loadSpec.keys.equals(["druid/bdp_druid_prod/algodash_map_row_report_agg/2016-09-02T00:00:00.000Z_2016-09-03T00:00:00.000Z/2016-10-10T21:19:50.893Z/0/index.zip"])
    }

    private static String getJsonDatasource(){
        return "{\n" +
            "  \"name\": \"algodash_map_row_report_agg\",\n" +
            "  \"properties\": {\n" +
            "    \"created\": \"2017-10-27T18:31:49.103Z\"\n" +
            "  },\n" +
            "  \"segments\": [\n" +
            "    {\n" +
            "      \"dataSource\": \"algodash_map_row_report_agg\",\n" +
            "      \"interval\": \"2016-09-01T00:00:00.000Z/2016-09-02T00:00:00.000Z\",\n" +
            "      \"version\": \"2016-10-10T21:19:50.893Z\",\n" +
            "      \"loadSpec\": {\n" +
            "        \"type\": \"s3_zip\",\n" +
            "        \"bucket\": \"netflix-dataoven-prod-users\",\n" +
            "        \"key\": \"druid/bdp_druid_prod/algodash_map_row_report_agg/2016-09-01T00:00:00.000Z_2016-09-02T00:00:00.000Z/2016-10-10T21:19:50.893Z/0/index.zip\"\n" +
            "      },\n" +
            "      \"dimensions\": \"region_desc,row_position,location_page_desc,gps_model,location_row_desc,list_context,nrdportal_device_category,location_group_desc,dateint,country_iso_code\",\n" +
            "      \"metrics\": \"num_rows,num_plays,sum_view_duration,num_plays_by_requestid,sum_view_duration_by_requestid,num_plays_qualified,sum_view_duration_qualified,num_plays_by_requestid_qualified,sum_view_duration_by_requestid_qualified,sum_time_to_first_play,sum_time_to_first_qualified_play\",\n" +
            "      \"shardSpec\": {\n" +
            "        \"type\": \"none\"\n" +
            "      },\n" +
            "      \"binaryVersion\": 9,\n" +
            "      \"size\": 19140218,\n" +
            "      \"identifier\": \"algodash_map_row_report_agg_2016-09-01T00:00:00.000Z_2016-09-02T00:00:00.000Z_2016-10-10T21:19:50.893Z\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"dataSource\": \"algodash_map_row_report_agg\",\n" +
            "      \"interval\": \"2016-09-02T00:00:00.000Z/2016-09-03T00:00:00.000Z\",\n" +
            "      \"version\": \"2016-10-10T21:19:50.893Z\",\n" +
            "      \"loadSpec\": {\n" +
            "        \"type\": \"s3_zip\",\n" +
            "        \"bucket\": \"netflix-dataoven-prod-users\",\n" +
            "        \"key\": \"druid/bdp_druid_prod/algodash_map_row_report_agg/2016-09-02T00:00:00.000Z_2016-09-03T00:00:00.000Z/2016-10-10T21:19:50.893Z/0/index.zip\"\n" +
            "      },\n" +
            "      \"dimensions\": \"region_desc,row_position,location_page_desc,gps_model,location_row_desc,list_context,nrdportal_device_category,location_group_desc,dateint,country_iso_code\",\n" +
            "      \"metrics\": \"num_rows,num_plays,sum_view_duration,num_plays_by_requestid,sum_view_duration_by_requestid,num_plays_qualified,sum_view_duration_qualified,num_plays_by_requestid_qualified,sum_view_duration_by_requestid_qualified,sum_time_to_first_play,sum_time_to_first_qualified_play\",\n" +
            "      \"shardSpec\": {\n" +
            "        \"type\": \"none\"\n" +
            "      },\n" +
            "      \"binaryVersion\": 9,\n" +
            "      \"size\": 20380105,\n" +
            "      \"identifier\": \"algodash_map_row_report_agg_2016-09-02T00:00:00.000Z_2016-09-03T00:00:00.000Z_2016-10-10T21:19:50.893Z\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";
    }
}
