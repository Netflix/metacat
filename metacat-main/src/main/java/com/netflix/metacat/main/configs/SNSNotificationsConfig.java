/*
 *
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
 *
 */
package com.netflix.metacat.main.configs;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.services.notifications.sns.SNSNotificationServiceImpl;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for SNS Notifications.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Slf4j
@Configuration
@ConditionalOnProperty(value = "metacat.notifications.sns.enabled", havingValue = "true")
public class SNSNotificationsConfig {

    /**
     * An object mapper bean to use if none already exists.
     *
     * @return JSON object mapper
     */
    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    /**
     * If SNS notifications are desired and no existing client has been created elsewhere
     * in the application create a default client here.
     *
     * @return The configured SNS client
     */
    //TODO: See what spring-cloud-aws would provide automatically...
    @Bean
    @ConditionalOnMissingBean
    public AmazonSNS amazonSNS() {
        return AmazonSNSClientBuilder.defaultClient();
    }

    /**
     * SNS Notification Publisher.
     *
     * @param amazonSNS    The SNS client to use
     * @param config       The system configuration abstraction to use
     * @param objectMapper The object mapper to use
     * @param registry     registry for spectator
     * @return Configured Notification Service bean
     */
    @Bean
    public SNSNotificationServiceImpl snsNotificationService(
        final AmazonSNS amazonSNS,
        final Config config,
        final ObjectMapper objectMapper,
        final Registry registry
    ) {
        final String tableArn = config.getSnsTopicTableArn();
        if (StringUtils.isEmpty(tableArn)) {
            throw new IllegalStateException(
                "SNS Notifications are enabled but no table ARN provided. Unable to configure."
            );
        }
        final String partitionArn = config.getSnsTopicPartitionArn();
        if (StringUtils.isEmpty(partitionArn)) {
            throw new IllegalStateException(
                "SNS Notifications are enabled but no partition ARN provided. Unable to configure."
            );
        }

        log.info("SNS notifications are enabled. Creating SNSNotificationServiceImpl bean.");
        return new SNSNotificationServiceImpl(amazonSNS, tableArn, partitionArn, objectMapper, registry);
    }
}
