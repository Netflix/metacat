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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.model.InvalidParameterException;
import com.amazonaws.services.sns.model.InvalidParameterValueException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.dto.notifications.sns.SNSMessage;
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
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.main.services.notifications.NotificationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;

import javax.validation.constraints.Size;
import java.io.IOException;
import java.util.UUID;

/**
 * Implementation of the NotificationService using Amazon SNS.
 *
 * @author tgianos
 * @since 0.1.47
 */
@Slf4j
public class SNSNotificationServiceImpl implements NotificationService {

    private final AmazonSNSAsync client;
    private final String tableTopicArn;
    private final String partitionTopicArn;
    private final ObjectMapper mapper;
    private final Config config;
    private SNSNotificationMetric notificationMetric;

    /**
     * Constructor.
     *
     * @param client             The SNS client to use to publish notifications
     * @param tableTopicArn      The topic to publish table related notifications to
     * @param partitionTopicArn  The topic to publish partition related notifications to
     * @param mapper             The object mapper to use to convert objects to JSON strings
     * @param config             The system config
     * @param notificationMetric The SNS notification metric
     */
    public SNSNotificationServiceImpl(
        final AmazonSNSAsync client,
        @Size(min = 1) final String tableTopicArn,
        @Size(min = 1) final String partitionTopicArn,
        final ObjectMapper mapper,
        final Config config,
        final SNSNotificationMetric notificationMetric
    ) {

        this.client = client;
        this.tableTopicArn = tableTopicArn;
        this.partitionTopicArn = partitionTopicArn;
        this.mapper = mapper;
        this.config = config;
        this.notificationMetric = notificationMetric;
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
        final UpdateTablePartitionsMessage tableMessage = new UpdateTablePartitionsMessage(
            UUID.randomUUID().toString(),
            timestamp,
            requestId,
            name,
            new TablePartitionsUpdatePayload(event.getPartitions().size(), 0)
        );
        this.publishNotification(this.tableTopicArn, tableMessage, event.getName(),
            "Unable to publish table partition add notification",
            Metrics.CounterSNSNotificationTablePartitionAdd.name(), true);
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
                this.publishNotification(this.partitionTopicArn, message, event.getName(),
                    "Unable to publish partition creation notification",
                    Metrics.CounterSNSNotificationPartitionAdd.name(), true);
                log.debug("Published create partition message {} on {}", message, this.partitionTopicArn);
            }
        }
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
        final UpdateTablePartitionsMessage tableMessage = new UpdateTablePartitionsMessage(
            UUID.randomUUID().toString(),
            timestamp,
            requestId,
            name,
            new TablePartitionsUpdatePayload(0, event.getPartitionIds().size())
        );
        this.publishNotification(this.tableTopicArn, tableMessage, event.getName(),
            "Unable to publish table partition delete notification",
            Metrics.CounterSNSNotificationTablePartitionDelete.name(), true);
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
                this.publishNotification(this.partitionTopicArn, message, event.getName(),
                    "Unable to publish partition deletion notification",
                    Metrics.CounterSNSNotificationPartitionDelete.name(), true);
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
        this.publishNotification(this.tableTopicArn, message, event.getName(),
            "Unable to publish create table notification",
            Metrics.CounterSNSNotificationTableCreate.name(), true);
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
        this.publishNotification(this.tableTopicArn, message, event.getName(),
            "Unable to publish delete table notification",
            Metrics.CounterSNSNotificationTableDelete.name(), true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfTableRename(final MetacatRenameTablePostEvent event) {
        log.debug("Received RenameTableEvent {}", event);
        UpdateTableMessage message = null;
        try {
            message = this.createUpdateTableMessage(
                UUID.randomUUID().toString(),
                event.getRequestContext().getTimestamp(),
                event.getRequestContext().getId(),
                event.getName().toString(),
                event.getOldTable(),
                event.getCurrentTable()
            );
            this.publishNotification(this.tableTopicArn, message, event.getName(),
                "Unable to publish rename table notification",
                Metrics.CounterSNSNotificationTableRename.name(), true);
        } catch (final Exception e) {
            this.notificationMetric.handleException(
                event.getName(),
                "Unable to create json patch for rename table notification",
                Metrics.CounterSNSNotificationTableRename.name(),
                message,
                e
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfTableUpdate(final MetacatUpdateTablePostEvent event) {
        log.debug("Received UpdateTableEvent {}", event);
        UpdateTableMessage message = null;
        try {
            message = this.createUpdateTableMessage(
                UUID.randomUUID().toString(),
                event.getRequestContext().getTimestamp(),
                event.getRequestContext().getId(),
                event.getName().toString(),
                event.getOldTable(),
                event.getCurrentTable()
            );
            this.publishNotification(this.tableTopicArn, message, event.getName(),
                "Unable to publish update table notification",
                Metrics.CounterSNSNotificationTableUpdate.name(), true);
        } catch (final Exception e) {
            this.notificationMetric.handleException(
                event.getName(),
                "Unable to create json patch for update table notification",
                Metrics.CounterSNSNotificationTableUpdate.name(),
                message,
                e
            );
        }
    }

    private UpdateTableMessage createUpdateTableMessage(
        final String id,
        final long timestamp,
        final String requestId,
        final String name,
        final TableDto oldTable,
        final TableDto currentTable
    ) throws IOException {
        final JsonPatch patch = JsonDiff.asJsonPatch(
            this.mapper.valueToTree(oldTable),
            this.mapper.valueToTree(currentTable)
        );
        return new UpdateTableMessage(
            id,
            timestamp,
            requestId,
            name,
            new UpdatePayload<>(oldTable, patch)
        );
    }

    private void publishNotification(
        final String arn,
        final SNSMessage<?> message,
        final QualifiedName name,
        final String errorMessage,
        final String counterKey,
        final boolean retryOnLongMessage
    ) {
        this.notificationMetric.recordTime(message, Metrics.TimerNotificationsBeforePublishDelay.getMetricName());
        try {
            final AsyncHandler<PublishRequest, PublishResult> handler =
                new AsyncHandler<PublishRequest, PublishResult>() {
                    @Override
                    public void onError(final Exception exception) {
                        if (retryOnLongMessage && (exception instanceof InvalidParameterException
                            || exception instanceof InvalidParameterValueException)) {
                            log.error("SNS Publish message exceeded the size threshold", exception);
                            notificationMetric.counterIncrement(
                                Metrics.CounterSNSNotificationPublishMessageSizeExceeded.getMetricName());
                            final SNSMessage<Void> voidMessage = new SNSMessage<>(message.getId(),
                                message.getTimestamp(), message.getRequestId(), message.getType(), message.getName(),
                                null);
                            publishNotification(arn, voidMessage, name, errorMessage, counterKey, false);
                        } else {
                            notificationMetric.handleException(name, errorMessage, counterKey, message, exception);
                        }
                    }

                    @Override
                    public void onSuccess(final PublishRequest request, final PublishResult publishResult) {
                        log.info("Successfully published message to topic {} with id {}",
                            arn, publishResult.getMessageId());
                        log.debug("Successfully published message {} to topic {} with id {}",
                            message, arn, publishResult.getMessageId());
                        notificationMetric.counterIncrement(counterKey);
                        notificationMetric.recordTime(message,
                            Metrics.TimerNotificationsPublishDelay.getMetricName());
                    }
                };
            client.publishAsync(arn, mapper.writeValueAsString(message), handler);
        } catch (final Exception e) {
            notificationMetric.handleException(name, errorMessage, counterKey, message, e);
        }
    }
}
