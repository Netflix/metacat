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

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.PublishResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.dto.notifications.sns.SNSMessage;
import com.netflix.metacat.common.dto.notifications.sns.SNSMessageType;
import com.netflix.metacat.common.dto.notifications.sns.messages.AddPartitionMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.CreateTableMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.DeletePartitionMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.DeleteTableMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.UpdateTableMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.UpdateTablePartitionsMessage;
import com.netflix.metacat.common.dto.notifications.sns.payloads.TablePartitionsUpdatePayload;
import com.netflix.metacat.common.dto.notifications.sns.payloads.UpdatePayload;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionMetadataOnlyPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.main.services.notifications.NotificationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;

import javax.annotation.Nullable;
import javax.validation.constraints.Size;
import java.util.UUID;

/**
 * Implementation of the NotificationService using Amazon SNS.
 *
 * @author tgianos
 * @since 0.1.47
 */
@Slf4j
public class SNSNotificationServiceImpl implements NotificationService {
    private final AmazonSNS client;
    private final String tableTopicArn;
    private final String partitionTopicArn;
    private final ObjectMapper mapper;
    private final Config config;
    private SNSNotificationMetric notificationMetric;
    private SNSNotificationServiceUtil snsNotificationServiceUtil;


    /**
     * Constructor.
     *
     * @param client                     The SNS client to use to publish notifications
     * @param tableTopicArn              The topic to publish table related notifications to
     * @param partitionTopicArn          The topic to publish partition related notifications to
     * @param mapper                     The object mapper to use to convert objects to JSON strings
     * @param config                     The system config
     * @param notificationMetric         The SNS notification metric
     * @param snsNotificationServiceUtil The SNS notification service util
     */
    public SNSNotificationServiceImpl(
        final AmazonSNS client,
        @Size(min = 1) final String tableTopicArn,
        @Size(min = 1) final String partitionTopicArn,
        final ObjectMapper mapper,
        final Config config,
        final SNSNotificationMetric notificationMetric,
        final SNSNotificationServiceUtil snsNotificationServiceUtil
    ) {

        this.client = client;
        this.tableTopicArn = tableTopicArn;
        this.partitionTopicArn = partitionTopicArn;
        this.mapper = mapper;
        this.config = config;
        this.notificationMetric = notificationMetric;
        this.snsNotificationServiceUtil = snsNotificationServiceUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfPartitionAddition(final MetacatSaveTablePartitionPostEvent event) {
        log.debug("Received SaveTablePartitionPostEvent {}", event);
        final String name = event.getName().toString();
        final long timestamp = event.getRequestContext().getTimestamp();
        final String requestId = event.getRequestContext().getId();
        // Publish a global message stating how many partitions were updated for the table to the table topic
        final TablePartitionsUpdatePayload partitionsUpdatePayload;
        if (this.config.isSnsNotificationAttachPartitionIdsEnabled() && event.getPartitions() != null) {
            partitionsUpdatePayload = this.snsNotificationServiceUtil.
                createTablePartitionsUpdatePayload(event.getPartitions(), event);
        } else {
            partitionsUpdatePayload = new TablePartitionsUpdatePayload(null, event.getPartitions().size(), 0,
                SNSNotificationPartitionAddMsg.PARTITION_KEY_UNABLED.name());
        }
        final UpdateTablePartitionsMessage tableMessage = new UpdateTablePartitionsMessage(
            UUID.randomUUID().toString(),
            timestamp,
            requestId,
            name,
            partitionsUpdatePayload);
        this.publishNotification(this.tableTopicArn, this.config.getFallbackSnsTopicTableArn(),
            tableMessage, event.getName(),
            "Unable to publish table partition add notification",
            Metrics.CounterSNSNotificationTablePartitionAdd.getMetricName());
        //publish the delete column key metric after publishing message
        if (this.config.isSnsNotificationAttachPartitionIdsEnabled()) {
                this.notificationMetric.recordPartitionLatestDeleteColumn(
                    event.getName(), partitionsUpdatePayload.getLatestDeleteColumnValue(),
                    partitionsUpdatePayload.getMessage());
        }
        if (config.isSnsNotificationTopicPartitionEnabled()) {
            AddPartitionMessage message = null;
            for (final PartitionDto partition : event.getPartitions()) {
                message = new AddPartitionMessage(
                    UUID.randomUUID().toString(),
                    timestamp,
                    requestId,
                    name,
                    partition
                );
                this.publishNotification(this.partitionTopicArn, this.config.getFallbackSnsTopicPartitionArn(),
                    message, event.getName(),
                    "Unable to publish partition creation notification",
                    Metrics.CounterSNSNotificationPartitionAdd.getMetricName());
                log.debug("Published create partition message {} on {}", message, this.partitionTopicArn);
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfPartitionMetdataDataSaveOnly(final MetacatSaveTablePartitionMetadataOnlyPostEvent event) {
        log.debug("Received SaveTablePartitionMetadataOnlyPostEvent {}", event);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfPartitionDeletion(final MetacatDeleteTablePartitionPostEvent event) {
        log.debug("Received DeleteTablePartition event {}", event);
        final String name = event.getName().toString();
        final long timestamp = event.getRequestContext().getTimestamp();
        final String requestId = event.getRequestContext().getId();
        final TablePartitionsUpdatePayload partitionsUpdatePayload;
        partitionsUpdatePayload = new TablePartitionsUpdatePayload(null, 0, event.getPartitions().size(),
            SNSNotificationPartitionAddMsg.PARTITION_KEY_UNABLED.name());

        final UpdateTablePartitionsMessage tableMessage = new UpdateTablePartitionsMessage(
            UUID.randomUUID().toString(),
            timestamp,
            requestId,
            name,
            partitionsUpdatePayload
        );
        this.publishNotification(this.tableTopicArn, this.config.getFallbackSnsTopicTableArn(),
            tableMessage, event.getName(),
            "Unable to publish table partition delete notification",
            Metrics.CounterSNSNotificationTablePartitionDelete.getMetricName());
        DeletePartitionMessage message = null;
        if (config.isSnsNotificationTopicPartitionEnabled()) {
            for (final String partitionId : event.getPartitionIds()) {
                message = new DeletePartitionMessage(
                    UUID.randomUUID().toString(),
                    timestamp,
                    requestId,
                    name,
                    partitionId
                );
                this.publishNotification(this.partitionTopicArn, this.config.getFallbackSnsTopicPartitionArn(),
                    message, event.getName(),
                    "Unable to publish partition deletion notification",
                    Metrics.CounterSNSNotificationPartitionDelete.getMetricName());
                log.debug("Published delete partition message {} on {}", message, this.partitionTopicArn);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfTableCreation(final MetacatCreateTablePostEvent event) {
        log.debug("Received CreateTableEvent {}", event);
        final CreateTableMessage message = new CreateTableMessage(
            UUID.randomUUID().toString(),
            event.getRequestContext().getTimestamp(),
            event.getRequestContext().getId(),
            event.getName().toString(),
            event.getTable()
        );
        this.publishNotification(this.tableTopicArn, this.config.getFallbackSnsTopicTableArn(),
            message, event.getName(),
            "Unable to publish create table notification",
            Metrics.CounterSNSNotificationTableCreate.getMetricName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfTableDeletion(final MetacatDeleteTablePostEvent event) {
        log.debug("Received DeleteTableEvent {}", event);
        final DeleteTableMessage message = new DeleteTableMessage(
            UUID.randomUUID().toString(),
            event.getRequestContext().getTimestamp(),
            event.getRequestContext().getId(),
            event.getName().toString(),
            event.getTable()
        );
        this.publishNotification(this.tableTopicArn, this.config.getFallbackSnsTopicTableArn(),
            message, event.getName(),
            "Unable to publish delete table notification",
            Metrics.CounterSNSNotificationTableDelete.getMetricName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfTableRename(final MetacatRenameTablePostEvent event) {
        log.debug("Received RenameTableEvent {}", event);
        final UpdateTableMessage message = this.createUpdateTableMessage(
            UUID.randomUUID().toString(),
            event.getRequestContext().getTimestamp(),
            event.getRequestContext().getId(),
            event.getName(),
            event.getOldTable(),
            event.getCurrentTable(),
            "Unable to create json patch for rename table notification",
            Metrics.CounterSNSNotificationTableRename.getMetricName()
        );
        this.publishNotification(this.tableTopicArn, this.config.getFallbackSnsTopicTableArn(),
            message, event.getName(),
            "Unable to publish rename table notification",
            Metrics.CounterSNSNotificationTableRename.getMetricName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfTableUpdate(final MetacatUpdateTablePostEvent event) {
        log.debug("Received UpdateTableEvent {}", event);
        final SNSMessage<?> message;
        final long timestamp = event.getRequestContext().getTimestamp();
        final String requestId = event.getRequestContext().getId();
        final QualifiedName name = event.getName();
        final TableDto oldTable = event.getOldTable();
        final TableDto currentTable = event.getCurrentTable();
        if (event.isLatestCurrentTable()) {
            message = this.createUpdateTableMessage(
                UUID.randomUUID().toString(),
                timestamp,
                requestId,
                name,
                oldTable,
                currentTable,
                "Unable to create json patch for update table notification",
                Metrics.CounterSNSNotificationTableUpdate.getMetricName()
            );
        } else {
            // Send a null payload if we failed to get the latest version
            // of the current table. This will signal users to callback
            //
            message = new SNSMessage<Void>(
                UUID.randomUUID().toString(),
                timestamp,
                requestId,
                SNSMessageType.TABLE_UPDATE,
                name.toString(),
                null);
        }

        this.publishNotification(this.tableTopicArn, this.config.getFallbackSnsTopicTableArn(),
            message, event.getName(),
            "Unable to publish update table notification",
            Metrics.CounterSNSNotificationTableUpdate.getMetricName());
    }

    private UpdateTableMessage createUpdateTableMessage(
        final String id,
        final long timestamp,
        final String requestId,
        final QualifiedName name,
        final TableDto oldTable,
        final TableDto currentTable,
        final String exceptionMessage,
        final String metricName
    ) {
        try {
            final JsonPatch patch = JsonDiff.asJsonPatch(
                this.mapper.valueToTree(oldTable),
                this.mapper.valueToTree(currentTable)
            );
            return new UpdateTableMessage(
                id,
                timestamp,
                requestId,
                name.toString(),
                new UpdatePayload<>(oldTable, patch)
            );
        } catch (final Exception e) {
            this.notificationMetric.handleException(
                name,
                exceptionMessage,
                metricName,
                null,
                e
            );
        }
        return null;
    }

    private void publishNotification(
        final String arn,
        @Nullable final String fallbackArn,
        final SNSMessage<?> message,
        final QualifiedName name,
        final String errorMessage,
        final String counterKey
    ) {
        this.notificationMetric.recordTime(message, Metrics.TimerNotificationsBeforePublishDelay.getMetricName());
        try {
            //
            // Publish the event to original SNS topic. If we receive an error from SNS, we will then try publishing
            // to the fallback topic.
            //
            try {
                publishNotification(arn, message, counterKey);
            } catch (final Exception exception) {
                if (fallbackArn != null) {
                    log.info("Fallback published message to topic {} because of error {}",
                        fallbackArn, exception.getMessage());
                    notificationMetric.counterIncrement(
                        Metrics.CounterSNSNotificationPublishFallback.getMetricName());
                    publishNotification(fallbackArn, message, counterKey);
                } else {
                    throw exception;
                }
            }
        } catch (Exception e) {
            notificationMetric.handleException(name, errorMessage, counterKey, message, e);
        }
    }

    private void publishNotification(
        final String arn,
        final SNSMessage<?> message,
        final String counterKey
    ) throws Exception {
        PublishResult result = null;
        try {
            result = this.client.publish(arn, this.mapper.writeValueAsString(message));
        } catch (Exception exception) {
            log.error("SNS Publish message failed.", exception);
            notificationMetric.counterIncrement(
                Metrics.CounterSNSNotificationPublishMessageSizeExceeded.getMetricName());
            final SNSMessage<Void> voidMessage = new SNSMessage<>(message.getId(),
                message.getTimestamp(), message.getRequestId(), message.getType(), message.getName(),
                null);
            result = this.client.publish(arn, this.mapper.writeValueAsString(voidMessage));
        }
        log.info("Successfully published message to topic {} with id {}", arn, result.getMessageId());
        log.debug("Successfully published message {} to topic {} with id {}", message, arn, result.getMessageId());
        notificationMetric.counterIncrement(counterKey);
        notificationMetric.recordTime(message, Metrics.TimerNotificationsPublishDelay.getMetricName());
    }
}
