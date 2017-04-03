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
package com.netflix.metacat.main.services.notifications.sns;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.main.services.notifications.DefaultNotificationServiceImpl;
import com.netflix.metacat.main.services.notifications.NotificationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

/**
 * Provides an instance of SNSNotificationServiceImpl if conditions are right.
 *
 * @author tgianos
 * @since 0.1.47
 */
@Slf4j
public class SNSNotificationServiceImplProvider implements Provider<NotificationService> {

    private final Config config;
    private final ObjectMapper mapper;

    /**
     * Constructor.
     *
     * @param config The metacat configuration
     * @param mapper The JSON object mapper to use
     */
    @Inject
    public SNSNotificationServiceImplProvider(@NotNull final Config config, @NotNull final ObjectMapper mapper) {
        this.config = config;
        this.mapper = mapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NotificationService get() {
        if (this.config.isSnsNotificationEnabled()) {
            final String tableArn = this.config.getSnsTopicTableArn();
            if (StringUtils.isEmpty(tableArn)) {
                throw new ProvisionException(
                    "SNS Notifications are enabled but no table ARN provided. Unable to configure."
                );
            }
            final String partitionArn = this.config.getSnsTopicPartitionArn();
            if (StringUtils.isEmpty(partitionArn)) {
                throw new ProvisionException(
                    "SNS Notifications are enabled but no partition ARN provided. Unable to configure."
                );
            }

            log.info("SNS notifications are enabled. Providing SNSNotificationServiceImpl implementation.");
            return new SNSNotificationServiceImpl(new AmazonSNSClient(), tableArn, partitionArn, this.mapper);
        } else {
            log.info("SNS notifications are not enabled. Ignoring and providing default implementation.");
            return new DefaultNotificationServiceImpl();
        }
    }
}
