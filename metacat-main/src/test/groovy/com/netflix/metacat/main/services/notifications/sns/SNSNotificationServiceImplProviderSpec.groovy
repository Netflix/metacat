/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.main.services.notifications.sns

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.ProvisionException
import com.netflix.metacat.common.server.Config
import com.netflix.metacat.main.services.notifications.DefaultNotificationServiceImpl
import spock.lang.Specification

/**
 * Tests for SNSNotificationsServiceImplProvider.
 *
 * @author tgianos
 * @since 0.1.47
 */
class SNSNotificationServiceImplProviderSpec extends Specification {

    def config = Mock(Config)
    def mapper = Mock(ObjectMapper)

    def "Will provide default implementation when SNS is disabled"() {
        def provider = new SNSNotificationServiceImplProvider(config, Mock(ObjectMapper))

        when: "call get"
        def service = provider.get()

        then:
        service instanceof DefaultNotificationServiceImpl
        1 * config.isSnsNotificationEnabled() >> false
        0 * config.getSnsTopicPartitionArn()
        0 * config.getSnsTopicTableArn()
    }

    def "Will provide SNS implementation when SNS is enabled"() {
        def provider = new SNSNotificationServiceImplProvider(this.config, this.mapper)

        when: "call get"
        def service = provider.get()

        then: "Should return a SNSNotificationServiceImpl implementation"
        service instanceof SNSNotificationServiceImpl
        1 * this.config.isSnsNotificationEnabled() >> true
        1 * this.config.getSnsTopicPartitionArn() >> UUID.randomUUID().toString()
        1 * this.config.getSnsTopicTableArn() >> UUID.randomUUID().toString()
    }

    def "Will throw exception if partition ARN not set but SNS enabled"() {
        def provider = new SNSNotificationServiceImplProvider(this.config, this.mapper)

        when: "call get"
        provider.get()

        then: "Will throw a provision exception"
        thrown ProvisionException
        1 * this.config.isSnsNotificationEnabled() >> true
        1 * this.config.getSnsTopicPartitionArn()
        1 * this.config.getSnsTopicTableArn() >> UUID.randomUUID().toString()
    }

    def "Will throw exception if table ARN not set but SNS enabled"() {
        def provider = new SNSNotificationServiceImplProvider(this.config, this.mapper)

        when: "call get"
        provider.get()

        then: "will throw a provision exception"
        thrown ProvisionException
        1 * this.config.isSnsNotificationEnabled() >> true
        0 * this.config.getSnsTopicPartitionArn()
        1 * this.config.getSnsTopicTableArn()
    }
}
