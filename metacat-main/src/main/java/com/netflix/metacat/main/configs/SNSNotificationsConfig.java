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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.main.services.notifications.sns.SNSNotificationMetric;
import com.netflix.metacat.main.services.notifications.sns.SNSNotificationServiceImpl;
import com.netflix.metacat.main.services.notifications.sns.SNSNotificationServiceUtil;
import io.micrometer.core.instrument.MeterRegistry;
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
     * If SNS notifications are desired and no existing client has been created elsewhere
     * in the application create a default client here.
     *
     * @return The configured SNS client
     */
    //TODO: See what spring-cloud-aws would provide automatically...
    @Bean
    @ConditionalOnMissingBean(AmazonSNS.class)
    public AmazonSNS amazonSNS() {
        return new AmazonSNSClient(DefaultAWSCredentialsProviderChain.getInstance());
    }

    /**
     * SNS Notification Publisher.
     *
     * @param amazonSNS                  The SNS client to use
     * @param config                     The system configuration abstraction to use
     * @param objectMapper               The object mapper to use
     * @param snsNotificationMetric      The sns notification metric
     * @param snsNotificationServiceUtil The SNS notification util
     * @return Configured Notification Service bean
     */
    @Bean
    public SNSNotificationServiceImpl snsNotificationService(
        final AmazonSNS amazonSNS,
        final Config config,
        final ObjectMapper objectMapper,
        final SNSNotificationMetric snsNotificationMetric,
        final SNSNotificationServiceUtil snsNotificationServiceUtil
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
        return new SNSNotificationServiceImpl(amazonSNS,
            tableArn, partitionArn, objectMapper, config, snsNotificationMetric, snsNotificationServiceUtil);
    }

    /**
     * SNS Notification Service Util.
     *
     * @param userMetadataService user metadata service
     * @return SNSNotificationServiceUtil
     */
    @Bean
    public SNSNotificationServiceUtil snsNotificationServiceUtil(
        final UserMetadataService userMetadataService
    ) {
        return new SNSNotificationServiceUtil(userMetadataService);
    }

    /**
     * SNS Notification Metric.
     *
     * @param registry registry for micrometer
     * @return Notification Metric bean
     */
    @Bean
    public SNSNotificationMetric snsNotificationMetric(
        final MeterRegistry registry
    ) {
        return new SNSNotificationMetric(registry);
    }
}
