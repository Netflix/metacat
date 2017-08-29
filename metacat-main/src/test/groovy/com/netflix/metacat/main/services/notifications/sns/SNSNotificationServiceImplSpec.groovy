/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.main.services.notifications.sns

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sns.AmazonSNSAsyncClient
import com.amazonaws.services.sns.model.NotFoundException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.TextNode
import com.google.common.collect.Lists
import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.PartitionDto
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.dto.notifications.sns.messages.*
import com.netflix.metacat.common.server.events.*
import com.netflix.metacat.common.server.properties.Config
import com.netflix.spectator.api.*
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Tests for the SNSNotificationServiceImpl.
 *
 * @author tgianos
 * @since 0.1.47
 */
class SNSNotificationServiceImplSpec extends Specification {
    def client = Mock(AmazonSNSAsyncClient)
    def qName = QualifiedName.fromString(
        UUID.randomUUID().toString()
            + "/"
            + UUID.randomUUID().toString()
            + "/"
            + UUID.randomUUID().toString()
            + "/"
            + UUID.randomUUID().toString()
            + "/"
            + UUID.randomUUID().toString()
    )
    def mapper = Mock(ObjectMapper)
    def partitionArn = UUID.randomUUID().toString()
    def tableArn = UUID.randomUUID().toString()
    def registry = Mock(Registry)
    def timer = Mock(Timer)
    def config = Mock(Config)
    def counter = Mock(Counter)
    def id = Mock(Id)
    def service;
    def clock = Mock(Clock)

    def requestContext = new MetacatRequestContext(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        "/mds/v1"
    )

    def setup() {
        this.registry.clock() >> clock
        this.clock.wallTime() >> System.currentTimeMillis()
        this.registry.timer(_, _, _) >> this.timer
        this.registry.counter(_) >> counter
        this.registry.createId(_) >> id
        this.service = new SNSNotificationServiceImpl(
            this.client,
            this.tableArn,
            this.partitionArn,
            this.mapper,
            this.config,
            new SNSNotificationMetric(this.registry)
        )
    }


    def "Will Notify On Partition Creation"() {
        def partitions = Lists.newArrayList(new PartitionDto(), new PartitionDto(), new PartitionDto())

        def event = new MetacatSaveTablePartitionPostEvent(
            this.qName,
            this.requestContext,
            this,
            partitions,
            Mock(PartitionsSaveResponseDto)
        )

        when:
        this.service.notifyOfPartitionAddition(event)

        then:
        3 * this.mapper.writeValueAsString(_ as AddPartitionMessage) >> UUID.randomUUID().toString()
        1 * this.mapper.writeValueAsString(_ as UpdateTablePartitionsMessage) >> UUID.randomUUID().toString()
        3 * this.client.publishAsync(this.partitionArn, _ as String, _ as AsyncHandler)
        1 * this.client.publishAsync(this.tableArn, _ as String, _ as AsyncHandler)
        4 * this.timer.record(_ as Long, _ as TimeUnit)
        1 * config.isSnsNotificationTopicPartitionEnabled() >> true
    }

    def "Will Notify On Partition Deletion"() {
        def partitions = Lists.newArrayList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )

        def event = new MetacatDeleteTablePartitionPostEvent(
            this.qName,
            this.requestContext,
            this,
            partitions
        )

        when:
        this.service.notifyOfPartitionDeletion(event)

        then:
        5 * this.mapper.writeValueAsString(_ as DeletePartitionMessage) >> UUID.randomUUID().toString()
        1 * this.mapper.writeValueAsString(_ as UpdateTablePartitionsMessage) >> UUID.randomUUID().toString()
        5 * this.client.publishAsync(this.partitionArn, _ as String, _ as AsyncHandler)
        1 * this.client.publishAsync(this.tableArn, _ as String, _ as AsyncHandler)
        6 * this.timer.record(_ as Long, _ as TimeUnit)
        1 * config.isSnsNotificationTopicPartitionEnabled() >> true
    }

    def "Will Notify On Table Creation"() {
        def event = new MetacatCreateTablePostEvent(
            this.qName,
            this.requestContext,
            this,
            new TableDto()
        )

        when:
        this.service.notifyOfTableCreation(event)

        then:
        1 * this.mapper.writeValueAsString(_ as CreateTableMessage) >> UUID.randomUUID().toString()
        1 * this.client.publishAsync(this.tableArn, _ as String, _ as AsyncHandler)
        1 * this.timer.record(_ as Long, _ as TimeUnit)
    }

    def "Will Notify On Table Deletion"() {
        def event = new MetacatDeleteTablePostEvent(
            this.qName,
            this.requestContext,
            this,
            new TableDto()
        )

        when:
        this.service.notifyOfTableDeletion(event)

        then:
        1 * this.mapper.writeValueAsString(_ as DeleteTableMessage) >> UUID.randomUUID().toString()
        1 * this.client.publishAsync(this.tableArn, _ as String, _ as AsyncHandler)
        1 * this.timer.record(_ as Long, _ as TimeUnit)
    }

    def "Will Notify On Table Rename"() {
        def event = new MetacatRenameTablePostEvent(
            this.qName,
            this.requestContext,
            this,
            new TableDto(),
            new TableDto()
        )

        when:
        this.service.notifyOfTableRename(event)

        then:
        2 * this.mapper.valueToTree(_ as TableDto) >> new TextNode(UUID.randomUUID().toString())
        1 * this.mapper.writeValueAsString(_ as UpdateTableMessage) >> UUID.randomUUID().toString()
        1 * this.client.publishAsync(this.tableArn, _ as String, _ as AsyncHandler)
        1 * this.timer.record(_ as Long, _ as TimeUnit)
    }

    def "Will Notify On Table Update"() {
        def event = new MetacatUpdateTablePostEvent(
            this.qName,
            this.requestContext,
            this,
            new TableDto(),
            new TableDto()
        )

        when:
        this.service.notifyOfTableUpdate(event)

        then:
        2 * this.mapper.valueToTree(_ as TableDto) >> new TextNode(UUID.randomUUID().toString())
        1 * this.mapper.writeValueAsString(_ as UpdateTableMessage) >> UUID.randomUUID().toString()
        1 * this.client.publishAsync(this.tableArn, _ as String, _ as AsyncHandler)
        1 * this.timer.record(_ as Long, _ as TimeUnit)
    }

    def "Won't retry on Other Exception"() {
        def event = new MetacatCreateTablePostEvent(
            this.qName,
            this.requestContext,
            this,
            new TableDto()
        )

        when:
        this.service.notifyOfTableCreation(event)

        then:
        1 * this.mapper.writeValueAsString(_ as CreateTableMessage) >> UUID.randomUUID().toString()
        1 * this.client.publishAsync(this.tableArn, _ as String, _ as AsyncHandler) >> { throw new NotFoundException("Exception") }
        1 * this.timer.record(_ as Long, _ as TimeUnit)
    }
}
