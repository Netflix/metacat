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

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.model.FieldInfo
import com.netflix.metacat.common.type.BaseType
import com.netflix.metacat.connector.druid.converter.DruidConnectorInfoConverter
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * DruidConnectorTableServiceSpec
 * @author zhenl
 * @since 1.2.0
 */
class DruidConnectorTableServiceSpec extends Specification{
     @Shared
    DruidConnectorInfoConverter druidInfoConverter = new DruidConnectorInfoConverter("test")
    @Shared
    ConnectorRequestContext connectorContext = new ConnectorRequestContext(1, null)
    @Shared
    tableNames = [
        QualifiedName.ofTable("druid", "default", "devtable2"),
        QualifiedName.ofTable("druid", "default", "devtable3"),
        QualifiedName.ofTable("druid", "default", "testtable1"),
        QualifiedName.ofTable("druid", "default", "testtable2"),
    ]
    @Shared
    druidTableNames = [
        "testtable1",
        "testtable2",
        "devtable2",
        "devtable3"
    ]

    @Unroll
    def "Test for listNames"() {
        def client = Mock(MetacatDruidClient)
        def druidTableService = new DruidConnectorTableService(client, druidInfoConverter)

        when:
        def tbls = druidTableService.listNames(connectorContext,
            QualifiedName.ofDatabase("druid", "default"),
            null,
            order,
            null)

        then:
        1 * client.getAllDataSources() >> druidTableNames
        result == tbls

        where:
        order                                 | result
        new Sort(null, SortOrder.ASC)  | tableNames
        new Sort(null, SortOrder.DESC) | tableNames.reverse()
    }

    @Unroll
    def "Test for listNames tables with page"() {
        def client = Mock(MetacatDruidClient)
        def druidTableService = new DruidConnectorTableService(client, druidInfoConverter)

        when:
        def tbls = druidTableService.listNames(
            connectorContext, QualifiedName.ofDatabase("druid", "default"), prefix, null, pageable)

        then:
        1 * client.getAllDataSources() >> druidTableNames
        tbls == result

        where:
        prefix                                             | pageable           | result
        null                                               | new Pageable(2, 1) | [QualifiedName.ofTable("druid", "default", "testtable2"), QualifiedName.ofTable("druid", "default", "devtable2")]
        QualifiedName.ofDatabase("druid", "default")      | new Pageable(2, 1) | [QualifiedName.ofTable("druid", "default", "testtable2"), QualifiedName.ofTable("druid", "default", "devtable2")]
        QualifiedName.ofTable("druid", "default", "test") | new Pageable(2, 1) | [QualifiedName.ofTable("druid", "default", "testtable2")]
        QualifiedName.ofTable("druid", "default", "test") | new Pageable(1, 0) | [QualifiedName.ofTable("druid", "default", "testtable1")]
        QualifiedName.ofTable("druid", "default", "test") | new Pageable(0, 0) | []
    }

    def "Test for listNames tables exceptions"() {
        def client = Mock(MetacatDruidClient)
        def druidTableService = new DruidConnectorTableService(client, druidInfoConverter)

        when:
        druidTableService.listNames(
            connectorContext, QualifiedName.ofDatabase("druid", "default"), null, null, null)

        then:
        1 * client.getAllDataSources() >> { throw new Exception() }
        thrown ConnectorException
    }

    def "Test for get table"() {
        def name = QualifiedName.ofTable("druid", "default", "testtable1")
        def client = Mock(MetacatDruidClient)
        def druidTableService = new DruidConnectorTableService(client, druidInfoConverter)
        def datareturn = new MetacatJsonLocator().parseJsonObject(getSegmentJson())

        when:
        def tbls = druidTableService.get(connectorContext, name)

        then:
        1 * client.getLatestDataByName(_ as String) >> datareturn
        tbls.fields.size() == 21
        for ( FieldInfo field : tbls.fields ) {
            if ( field.getComment().equals(DruidConfigConstants.DIMENSIONS) ) {
                assert field.getType().equals(BaseType.STRING)
            }
            if ( field.getComment().equals(DruidConfigConstants.METRICS) ) {
                assert field.getType().equals(BaseType.DOUBLE)
            }
        }
    }

    private static String getSegmentJson() {
        return "{\n" +
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
            "}";
    }
}
