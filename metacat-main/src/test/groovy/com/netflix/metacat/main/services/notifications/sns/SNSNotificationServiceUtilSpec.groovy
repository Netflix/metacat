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

package com.netflix.metacat.main.services.notifications.sns

import com.google.common.collect.Lists
import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.PartitionDto
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import spock.lang.Specification
import spock.lang.Unroll

import java.text.ParseException

class SNSNotificationServiceUtilSpec extends Specification{
    def UserMetadataService userMetadataService = Mock(UserMetadataService)
    def metacatJsonLocator = new MetacatJsonLocator();
    def requestContext = MetacatRequestContext.builder().userName(UUID.randomUUID().toString())
        .clientAppName(UUID.randomUUID().toString())
        .clientId(UUID.randomUUID().toString())
        .jobId(UUID.randomUUID().toString())
        .dataTypeContext(UUID.randomUUID().toString())
        .apiUri("/mds/v1")
        .scheme("internal").build();

    def "Test get sorted deletion partitions"() {
        def SNSNotificationServiceUtil notificationServiceUtil = new SNSNotificationServiceUtil(userMetadataService)
        def partitions = Lists.newArrayList(
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table', 'dateint=20170710/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=20170712/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=20170713/hour=0/batchid=merged_1')).build(),
        )
        when:
        def ret = notificationServiceUtil.getSortedDeletionPartitionKeys(partitions, "dateint");

        then:
        assert ret == ['20170713', '20170712', '20170710']

        when:
        partitions = Lists.newArrayList(
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table', 'dateint=20170710/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=20170710/hour=2/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=20170710/hour=3/batchid=merged_1')).build(),
        )
        ret = notificationServiceUtil.getSortedDeletionPartitionKeys(partitions, "dateint");

        then:
        assert ret == ['20170710', '20170710', '20170710']

        when:
        partitions = Lists.newArrayList(
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table', 'window_endtime=1522257960000/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'window_endtime=1522257970000/hour=2/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'window_endtime=1522257980000/hour=3/batchid=merged_1')).build(),
        )
        ret = notificationServiceUtil.getSortedDeletionPartitionKeys(partitions, "window_endtime");

        then:
        assert ret == ['1522257980000', '1522257970000', '1522257960000']

    }

    @Unroll
    def "Test getTimestamp" (){
        def SNSNotificationServiceUtil notificationServiceUtil = new SNSNotificationServiceUtil(userMetadataService)

        when:
        def ret = notificationServiceUtil.getTimeStamp(timestr, regional)
        then:
        assert ret == output

        where:
        timestr                      | regional | output
        '20180501'                   | false    | 1525132800
        '20180501'                   | true     | 1525158000
        '1525132800'                 | false    | 1525132800
        '1525132800'                 | true     | 1525132800
        '1525132800.123345'          | false    | 1525132800
        '1525132800.123345'          | true     | 1525132800
        '1525132800000.123345'       | false    | 1525132800
        '1525132800000.123345'       | true     | 1525132800
    }

    @Unroll
    def "Test getTimestamp exception " (){
        def SNSNotificationServiceUtil notificationServiceUtil = new SNSNotificationServiceUtil(userMetadataService)

        when:
        def ret = notificationServiceUtil.getTimeStamp(timestr, regional)

        then:
        thrown ParseException

        where:
        timestr                        | regional
        '2018'                         | false
        '2018050'                      | true
        '152513280120'                 | false
        '1525132'                      | true
        '152500.123345'                | false
        '152513280000000.123345'       | false
    }

    def "Test get payload valid case" () {
        def SNSNotificationServiceUtil notificationServiceUtil = new SNSNotificationServiceUtil(userMetadataService)
        def partitions = Lists.newArrayList(
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table', 'dateint=20170710/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=29170712/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=20170713/hour=0/batchid=merged_1')).build(),
        )
        def str =  "{ \"data_dependency\": {\"partition_column_date_type\": \"region\"}," +
            "  \"data_hygiene\": {\"delete_column\":\"dateint\",\"delete_method\": \"by partition column\" }}"
        def node = metacatJsonLocator.parseJsonObject(str)
        def event = new MetacatSaveTablePartitionPostEvent(
            QualifiedName.fromString("testhive/test/test_table"),
            this.requestContext,
            this,
            partitions,
            Mock(PartitionsSaveResponseDto)
        )

        when:
        def payload = notificationServiceUtil.createTablePartitionsUpdatePayload(partitions, event)
        assert  payload.getLatestDeleteColumnValue() == 'dateint=20170713'
        assert payload.getNumCreatedPartitions() == 3
        assert payload.getMessage() == "ATTACHED_VALID_PARITITION_KEY"
        then:
        userMetadataService.getDefinitionMetadata(QualifiedName.fromString("testhive/test/test_table")) >> Optional.ofNullable(node)
    }

    def "Test get payload future key case" () {
        def SNSNotificationServiceUtil notificationServiceUtil = new SNSNotificationServiceUtil(userMetadataService)
        def partitions = Lists.newArrayList(
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table', 'dateint=29170710/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=29170712/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=29170713/hour=0/batchid=merged_1')).build(),
        )
        def str =  "{ \"data_dependency\": {\"partition_column_date_type\": \"region\"}," +
            "  \"data_hygiene\": {\"delete_column\":\"dateint\",\"delete_method\": \"by partition column\" }}"
        def node = metacatJsonLocator.parseJsonObject(str)
        def event = new MetacatSaveTablePartitionPostEvent(
            QualifiedName.fromString("testhive/test/test_table"),
            this.requestContext,
            this,
            partitions,
            Mock(PartitionsSaveResponseDto)
        )

        when:
        def payload = notificationServiceUtil.createTablePartitionsUpdatePayload(partitions, event)
        assert  payload.getLatestDeleteColumnValue() == null
        assert payload.getNumCreatedPartitions() == 3
        assert payload.getMessage() == "ALL_FUTURE_PARTITION_KEYS"
        then:
        userMetadataService.getDefinitionMetadata(QualifiedName.fromString("testhive/test/test_table")) >> Optional.ofNullable(node)
    }

    def "Test get payload invalid key case" () {
        def SNSNotificationServiceUtil notificationServiceUtil = new SNSNotificationServiceUtil(userMetadataService)
        def partitions = Lists.newArrayList(
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table', 'dateint=929170710/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=929170712/hour=0/batchid=merged_1')).build(),
            PartitionDto.builder().name(QualifiedName.ofPartition('testhive', 'test', 'test_table',  'dateint=929170713/hour=0/batchid=merged_1')).build(),
        )
        def str =  "{ \"data_dependency\": {\"partition_column_date_type\": \"region\"}," +
            "  \"data_hygiene\": {\"delete_column\":\"dateint\",\"delete_method\": \"by partition column\" }}"
        def node = metacatJsonLocator.parseJsonObject(str)
        def event = new MetacatSaveTablePartitionPostEvent(
            QualifiedName.fromString("testhive/test/test_table"),
            this.requestContext,
            this,
            partitions,
            Mock(PartitionsSaveResponseDto)
        )

        when:
        def payload = notificationServiceUtil.createTablePartitionsUpdatePayload(partitions, event)
        assert  payload.getLatestDeleteColumnValue() == null
        assert payload.getNumCreatedPartitions() == 3
        assert payload.getMessage() == "INVALID_PARTITION_KEY_FORMAT"
        then:
        userMetadataService.getDefinitionMetadata(QualifiedName.fromString("testhive/test/test_table")) >> Optional.ofNullable(node)
    }

}
