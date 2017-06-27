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
import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.netflix.metacat.main.services.notifications.NotificationService;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;

import javax.annotation.Nullable;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    private final Registry registry;

    /**
     * Constructor.
     *
     * @param client            The SNS client to use to publish notifications
     * @param tableTopicArn     The topic to publish table related notifications to
     * @param partitionTopicArn The topic to publish partition related notifications to
     * @param mapper            The object mapper to use to convert objects to JSON strings
     * @param registry          The registry handle of spectator
     */
    public SNSNotificationServiceImpl(
         final AmazonSNS client,
         @Size(min = 1) final String tableTopicArn,
         @Size(min = 1) final String partitionTopicArn,
         final ObjectMapper mapper,
         final Registry registry
    ) {

        this.client = client;
        this.tableTopicArn = tableTopicArn;
        this.partitionTopicArn = partitionTopicArn;
        this.mapper = mapper;
        this.registry = registry;
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
        AddPartitionMessage message = null;
        try {
            for (final PartitionDto partition : event.getPartitions()) {
                message = new AddPartitionMessage(
                    UUID.randomUUID().toString(),
                    timestamp,
                    requestId,
                    name,
                    partition
                );
                this.publishNotification(this.partitionTopicArn, message);
                log.debug("Published create partition message {} on {}", message, this.partitionTopicArn);
                this.registry.counter(
                    this.registry
                        .createId(Metrics.CounterSNSNotificationPartitionAdd.name())
                        .withTags(Metrics.statusSuccessMap)
                ).increment();
            }
        } catch (final Exception e) {
            this.handleException(
                event.getName(),
                "Unable to publish partition creation notification",
                Metrics.CounterSNSNotificationPartitionAdd.name(),
                message,
                e
            );
        }
        UpdateTablePartitionsMessage tableMessage = null;
        try {
            // Publish a global message stating how many partitions were updated for the table to the table topic
            tableMessage = new UpdateTablePartitionsMessage(
                UUID.randomUUID().toString(),
                timestamp,
                requestId,
                name,
                new TablePartitionsUpdatePayload(event.getPartitions().size(), 0)
            );
            this.publishNotification(this.tableTopicArn, tableMessage);
            this.registry.counter(
                this.registry
                    .createId(Metrics.CounterSNSNotificationTablePartitionAdd.name())
                    .withTags(Metrics.statusSuccessMap)
            ).increment();
        } catch (final Exception e) {
            this.handleException(
                event.getName(),
                "Unable to publish table partition add notification",
                Metrics.CounterSNSNotificationTablePartitionAdd.name(),
                tableMessage,
                e
            );
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
        DeletePartitionMessage message = null;
        try {
            for (final String partitionId : event.getPartitionIds()) {
                message = new DeletePartitionMessage(
                    UUID.randomUUID().toString(),
                    timestamp,
                    requestId,
                    name,
                    partitionId
                );
                this.publishNotification(this.partitionTopicArn, message);
                this.registry.counter(
                    this.registry
                        .createId(Metrics.CounterSNSNotificationPartitionDelete.name())
                        .withTags(Metrics.statusSuccessMap)
                ).increment();
                log.debug("Published delete partition message {} on {}", message, this.partitionTopicArn);
            }
        } catch (final Exception e) {
            handleException(
                event.getName(),
                "Unable to publish partition deletion notification",
                Metrics.CounterSNSNotificationPartitionDelete.name(),
                message,
                e
            );
        }
        UpdateTablePartitionsMessage tableMessage = null;
        try {
            // Publish a global message stating how many partitions were updated for the table to the table topic
            tableMessage = new UpdateTablePartitionsMessage(
                UUID.randomUUID().toString(),
                timestamp,
                requestId,
                name,
                new TablePartitionsUpdatePayload(0, event.getPartitionIds().size())
            );
            this.publishNotification(this.tableTopicArn, tableMessage);
            this.registry.counter(
                this.registry
                    .createId(Metrics.CounterSNSNotificationTablePartitionDelete.name())
                    .withTags(Metrics.statusSuccessMap)
            ).increment();
        } catch (final Exception e) {
            this.handleException(
                event.getName(),
                "Unable to publish table partition delete notification",
                Metrics.CounterSNSNotificationTablePartitionDelete.name(),
                tableMessage,
                e
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @EventListener
    public void notifyOfTableCreation(final MetacatCreateTablePostEvent event) {
        log.debug("Received CreateTableEvent {}", event);
        CreateTableMessage message = null;
        try {
            message = new CreateTableMessage(
                UUID.randomUUID().toString(),
                event.getRequestContext().getTimestamp(),
                event.getRequestContext().getId(),
                event.getName().toString(),
                event.getTable()
            );
            this.publishNotification(this.tableTopicArn, message);
            this.registry.counter(
                this.registry
                    .createId(Metrics.CounterSNSNotificationTableCreate.name())
                    .withTags(Metrics.statusSuccessMap)
            ).increment();
        } catch (final Exception e) {
            this.handleException(
                event.getName(),
                "Unable to publish create table notification",
                Metrics.CounterSNSNotificationTableCreate.name(),
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
    public void notifyOfTableDeletion(final MetacatDeleteTablePostEvent event) {
        log.debug("Received DeleteTableEvent {}", event);
        DeleteTableMessage message = null;
        try {
            message = new DeleteTableMessage(
                UUID.randomUUID().toString(),
                event.getRequestContext().getTimestamp(),
                event.getRequestContext().getId(),
                event.getName().toString(),
                event.getTable()
            );
            this.publishNotification(this.tableTopicArn, message);
            this.registry.counter(
                this.registry
                    .createId(Metrics.CounterSNSNotificationTableDelete.name())
                    .withTags(Metrics.statusSuccessMap)
            ).increment();
        } catch (final Exception e) {
            this.handleException(
                event.getName(),
                "Unable to publish delete table notification",
                Metrics.CounterSNSNotificationTableDelete.name(),
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
            this.publishNotification(this.tableTopicArn, message);
            this.registry.counter(
                this.registry
                    .createId(Metrics.CounterSNSNotificationTableRename.name())
                    .withTags(Metrics.statusSuccessMap)
            ).increment();
        } catch (final Exception e) {
            this.handleException(
                event.getName(),
                "Unable to publish rename table notification",
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
            this.publishNotification(this.tableTopicArn, message);
            this.registry.counter(
                this.registry
                    .createId(Metrics.CounterSNSNotificationTableUpdate.name())
                    .withTags(Metrics.statusSuccessMap)
            ).increment();
        } catch (final Exception e) {
            this.handleException(
                event.getName(),
                "Unable to publish update table notification",
                Metrics.CounterSNSNotificationTableUpdate.name(),
                message,
                e
            );
        }
    }

    private void handleException(
        final QualifiedName name,
        final String message,
        final String counterKey,
        @Nullable final SNSMessage payload,
        final Exception e
    ) {
        log.error("{} with payload: {}", message, payload, e);
        final Map<String, String> tags = new HashMap<>(name.parts());
        tags.putAll(Metrics.statusFailureMap);
        this.registry.counter(this.registry.createId(counterKey).withTags(tags)).increment();
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
            new UpdatePayload<>(oldTable, patch, currentTable)
        );
    }

    private void publishNotification(
        final String arn,
        final SNSMessage<?> message
    ) throws JsonProcessingException {
        try {
            final PublishResult result = this.client.publish(arn, this.mapper.writeValueAsString(message));
            log.debug("Successfully published message {} to topic {} with id {}", message, arn, result.getMessageId());
        } finally {
            final Timer timer = this.registry.timer(
                Metrics.TimerNotificationsPublishDelay.name(),
                "type",
                message.getClass().getName()
            );
            timer.record(System.currentTimeMillis() - message.getTimestamp(), TimeUnit.MILLISECONDS);
        }
    }
}
