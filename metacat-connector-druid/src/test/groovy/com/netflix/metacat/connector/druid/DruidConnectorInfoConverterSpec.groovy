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

package com.netflix.metacat.connector.druid

import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.connectors.model.FieldInfo
import com.netflix.metacat.common.type.BaseType
import com.netflix.metacat.connector.druid.converter.DruidConverterUtil
import com.netflix.metacat.connector.druid.converter.DruidConnectorInfoConverter
import spock.lang.Specification

/**
 * DruidConnectorInfoConverterSpec.
 * @author zhenl
 * @since 1.2.0
 */
class DruidConnectorInfoConverterSpec extends Specification{

    def "Test getDatasourceFromJsonObject" () {
        def druidInfoConverter = new DruidConnectorInfoConverter("test")
        def data = new MetacatJsonLocator().parseJsonObject(getDatasourceJson())

        when:
        def tbls = druidInfoConverter.getTableInfoFromDatasource(DruidConverterUtil.getDatasourceFromAllSegmentJsonObject(data))

        then:
        tbls.fields.size() == 21
        for ( FieldInfo field : tbls.fields ) {
            if ( field.getComment().equals(DruidConfigConstants.DIMENSIONS) ) {
                assert field.getType().equals(BaseType.STRING)
            }
            if ( field.getComment().equals(DruidConfigConstants.METRICS) ) {
                assert field.getType().equals(BaseType.DOUBLE)
            }
        }
        tbls.getName() != null
    }

    def "Test getPartitionInfoFromSegment" () {
        def druidInfoConverter = new DruidConnectorInfoConverter("test")
        def data = new MetacatJsonLocator().parseJsonObject(getDatasourceJson())

        when:
        def dataSource = DruidConverterUtil.getDatasourceFromAllSegmentJsonObject(data)
        def partitionInfo = druidInfoConverter.getPartitionInfoFromSegment(dataSource.getSegmentList().get(0))

        then:
        partitionInfo.metadata.get(DruidConfigConstants.METRICS).equals("num_rows,num_plays,sum_view_duration,num_plays_by_requestid" +
            ",sum_view_duration_by_requestid,num_plays_qualified,sum_view_duration_qualified,num_plays_by_requestid_qualified,sum_view_duration_by_requestid_qualified,sum_time_to_first_play,sum_time_to_first_qualified_play")
        partitionInfo.metadata.get(DruidConfigConstants.DIMENSIONS).equals("region_desc,row_position,location_page_desc," +
            "gps_model,location_row_desc,list_context,nrdportal_device_category,location_group_desc,dateint,country_iso_code")
    }

    private static getDatasourceJson() {
        return "{\n" +
            "\"name\": \"algodash_map_row_report_agg\",\n" +
            "\"properties\": {\n" +
            "\"created\": \"2017-09-27T16:41:15.154Z\"\n" +
            "},\n" +
            "\"segments\": [\n" +
            "{\n" +
            "\"dataSource\": \"algodash_map_row_report_agg\",\n" +
            "\"interval\": \"2016-09-01T00:00:00.000Z/2016-09-02T00:00:00.000Z\",\n" +
            "\"version\": \"2016-10-10T21:19:50.893Z\",\n" +
            "\"loadSpec\": {\n" +
            "\"type\": \"s3_zip\",\n" +
            "\"bucket\": \"netflix-dataoven-prod-users\",\n" +
            "\"key\": \"druid/bdp_druid_prod/algodash_map_row_report_agg/2016-09-01T00:00:00.000Z_2016-09-02T00:00:00.000Z/2016-10-10T21:19:50.893Z/0/index.zip\"\n" +
            "},\n" +
            "\"dimensions\": \"region_desc,row_position,location_page_desc,gps_model,location_row_desc,list_context,nrdportal_device_category,location_group_desc,dateint,country_iso_code\",\n" +
            "\"metrics\": \"num_rows,num_plays,sum_view_duration,num_plays_by_requestid,sum_view_duration_by_requestid,num_plays_qualified,sum_view_duration_qualified,num_plays_by_requestid_qualified,sum_view_duration_by_requestid_qualified,sum_time_to_first_play,sum_time_to_first_qualified_play\",\n" +
            "\"shardSpec\": {\n" +
            "\"type\": \"none\"\n" +
            "},\n" +
            "\"binaryVersion\": 9,\n" +
            "\"size\": 19140218,\n" +
            "\"identifier\": \"algodash_map_row_report_agg_2016-09-01T00:00:00.000Z_2016-09-02T00:00:00.000Z_2016-10-10T21:19:50.893Z\"\n" +
            "},\n" +
            "{\n" +
            "\"dataSource\": \"algodash_map_row_report_agg\",\n" +
            "\"interval\": \"2016-09-02T00:00:00.000Z/2016-09-03T00:00:00.000Z\",\n" +
            "\"version\": \"2016-10-10T21:19:50.893Z\",\n" +
            "\"loadSpec\": {\n" +
            "\"type\": \"s3_zip\",\n" +
            "\"bucket\": \"netflix-dataoven-prod-users\",\n" +
            "\"key\": \"druid/bdp_druid_prod/algodash_map_row_report_agg/2016-09-02T00:00:00.000Z_2016-09-03T00:00:00.000Z/2016-10-10T21:19:50.893Z/0/index.zip\"\n" +
            "},\n" +
            "\"dimensions\": \"region_desc,row_position,location_page_desc,gps_model,location_row_desc,list_context,nrdportal_device_category,location_group_desc,dateint,country_iso_code\",\n" +
            "\"metrics\": \"num_rows,num_plays,sum_view_duration,num_plays_by_requestid,sum_view_duration_by_requestid,num_plays_qualified,sum_view_duration_qualified,num_plays_by_requestid_qualified,sum_view_duration_by_requestid_qualified,sum_time_to_first_play,sum_time_to_first_qualified_play\",\n" +
            "\"shardSpec\": {\n" +
            "\"type\": \"none\"\n" +
            "},\n" +
            "\"binaryVersion\": 9,\n" +
            "\"size\": 20380105,\n" +
            "\"identifier\": \"algodash_map_row_report_agg_2016-09-02T00:00:00.000Z_2016-09-03T00:00:00.000Z_2016-10-10T21:19:50.893Z\"\n" +
            "}]\n" +
            "}";
    }
}
