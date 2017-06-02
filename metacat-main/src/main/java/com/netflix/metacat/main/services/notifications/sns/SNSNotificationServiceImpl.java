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
import com.amazonaws.services.sns.AmazonSNSAsyncClient;
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
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.monitoring.CounterWrapper;
import com.netflix.metacat.main.services.notifications.NotificationService;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.DynamicTimer;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.BasicTagList;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
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

    private final AmazonSNSAsyncClient client;
    private final String tableTopicArn;
    private final String partitionTopicArn;
    private final ObjectMapper mapper;
    private final Config config;

    /**
     * Constructor.
     *
     * @param client            The SNS client to use to publish notifications
     * @param tableTopicArn     The topic to publish table related notifications to
     * @param partitionTopicArn The topic to publish partition related notifications to
     * @param mapper            The object mapper to use to convert objects to JSON strings
     * @param config            Server configuration
     */
    public SNSNotificationServiceImpl(
        @Nonnull final AmazonSNSAsyncClient client,
        @Nonnull @Size(min = 1) final String tableTopicArn,
        @Nonnull @Size(min = 1) final String partitionTopicArn,
        @Nonnull final ObjectMapper mapper,
        @Nonnull final Config config
    ) {
        this.client = client;
        this.tableTopicArn = tableTopicArn;
        this.partitionTopicArn = partitionTopicArn;
        this.mapper = mapper;
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfPartitionAddition(@Nonnull final MetacatSaveTablePartitionPostEvent event) {
        log.debug("Received SaveTablePartitionPostEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.partitions.add");
        final String name = event.getName().toString();
        final long timestamp = event.getRequestContext().getTimestamp();
        final String requestId = event.getRequestContext().getId();
        final UpdateTablePartitionsMessage tableMessage = new UpdateTablePartitionsMessage(
            UUID.randomUUID().toString(),
            timestamp,
            requestId,
            name,
            new TablePartitionsUpdatePayload(
                event.getPartitions().size(),
                0
            )
        );
        this.publishNotification(this.tableTopicArn, tableMessage, event.getName(),
            "Unable to publish table partition add notification",
            "metacat.notifications.sns.tables.addPartitions.failed", true);
        // TODO: In ideal world this this be an injected object to the class so we can mock for tests
        //       swap out implementations etc.
        CounterWrapper.incrementCounter("metacat.notifications.sns.tables.addPartitions.succeeded");
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
                    "metacat.notifications.sns.partitions.add.failed", true);
                log.debug("Published create partition message {} on {}", message, this.partitionTopicArn);
                CounterWrapper.incrementCounter("metacat.notifications.sns.partitions.add.succeeded");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfPartitionDeletion(@Nonnull final MetacatDeleteTablePartitionPostEvent event) {
        log.debug("Received DeleteTablePartition event {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.partitions.delete");
        final String name = event.getName().toString();
        final long timestamp = event.getRequestContext().getTimestamp();
        final String requestId = event.getRequestContext().getId();
        final UpdateTablePartitionsMessage tableMessage = new UpdateTablePartitionsMessage(
            UUID.randomUUID().toString(),
            timestamp,
            requestId,
            name,
            new TablePartitionsUpdatePayload(
                0,
                event.getPartitionIds().size()
            )
        );
        this.publishNotification(this.tableTopicArn, tableMessage, event.getName(),
            "Unable to publish table partition delete notification",
            "metacat.notifications.sns.tables.deletePartitions.failed", true);
        CounterWrapper.incrementCounter("metacat.notifications.sns.tables.deletePartitions.succeeded");
        if (config.isSnsNotificationTopicPartitionEnabled()) {
            DeletePartitionMessage message = null;
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
                    "metacat.notifications.sns.partitions.delete.failed", true);
                CounterWrapper.incrementCounter("metacat.notifications.sns.partitions.delete.succeeded");
                log.debug("Published delete partition message {} on {}", message, this.partitionTopicArn);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableCreation(@Nonnull final MetacatCreateTablePostEvent event) {
        log.debug("Received CreateTableEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.tables.create");
        final CreateTableMessage message = new CreateTableMessage(
            UUID.randomUUID().toString(),
            event.getRequestContext().getTimestamp(),
            event.getRequestContext().getId(),
            event.getName().toString(),
            event.getTable()
        );
        this.publishNotification(this.tableTopicArn, message, event.getName(),
            "Unable to publish create table notification",
            "metacat.notifications.sns.tables.create.failed", true);
        CounterWrapper.incrementCounter("metacat.notifications.sns.tables.create.succeeded");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableDeletion(@Nonnull final MetacatDeleteTablePostEvent event) {
        log.debug("Received DeleteTableEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.tables.delete");
        final DeleteTableMessage message = new DeleteTableMessage(
            UUID.randomUUID().toString(),
            event.getRequestContext().getTimestamp(),
            event.getRequestContext().getId(),
            event.getName().toString(),
            event.getTable()
        );
        this.publishNotification(this.tableTopicArn, message, event.getName(),
            "Unable to publish delete table notification",
            "metacat.notifications.sns.tables.delete.failed", true);
        CounterWrapper.incrementCounter("metacat.notifications.sns.tables.delete.succeeded");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableRename(@Nonnull final MetacatRenameTablePostEvent event) {
        log.debug("Received RenameTableEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.tables.rename");
        final UpdateTableMessage message;
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
                "metacat.notifications.sns.tables.rename.failed", true);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.rename.succeeded");
        } catch (IOException e) {
            handleException(event.getName(), "Unable to create json patch for rename table notification",
                "metacat.notifications.sns.tables.rename.json.failed", null, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableUpdate(@Nonnull final MetacatUpdateTablePostEvent event) {
        log.debug("Received UpdateTableEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.tables.update");
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
                "metacat.notifications.sns.tables.update.failed", true);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.update.succeeded");
        } catch (final Exception e) {
            handleException(event.getName(), "Unable to create json patch for update table notification",
                "metacat.notifications.sns.tables.update.json.failed", message, e);
        }
    }

    private void handleException(final QualifiedName name, final String message, final String counterKey,
                                 final SNSMessage payload, final Exception e) {
        log.error("{} with payload: {}", message, payload, e);
        DynamicCounter.increment(counterKey, BasicTagList.copyOf(name.parts()));
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
        DynamicTimer.record(MonitorConfig.builder("metacat.notifications.sns.before.publish.delay")
                .withTag("metacat.event.type", message.getType().name()).build(),
            System.currentTimeMillis() - message.getTimestamp());
        try {
            final AsyncHandler<PublishRequest, PublishResult> handler =
                new AsyncHandler<PublishRequest, PublishResult>() {
                    @Override
                    public void onError(final Exception exception) {
                        if (retryOnLongMessage && (exception instanceof InvalidParameterException
                            || exception instanceof InvalidParameterValueException)) {
                            log.error("SNS Publish message exceeded the size threshold", exception);
                            CounterWrapper.incrementCounter("metacat.notifications.sns.publish.message.size.exceeded");
                            final SNSMessage<Void> voidMessage = new SNSMessage<>(message.getId(),
                                message.getTimestamp(), message.getRequestId(), message.getType(), message.getName(),
                                null);
                            publishNotification(arn, voidMessage, name, errorMessage, counterKey, false);
                        } else {
                            handleException(name, errorMessage, counterKey, message, exception);
                        }
                    }

                    @Override
                    public void onSuccess(final PublishRequest request, final PublishResult publishResult) {
                        log.info("Successfully published message to topic {} with id {}",
                            arn, publishResult.getMessageId());
                        log.debug("Successfully published message {} to topic {} with id {}",
                            message, arn, publishResult.getMessageId());
                        DynamicTimer.record(MonitorConfig.builder("metacat.notifications.sns.publish.delay")
                                .withTag("metacat.event.type", message.getType().name()).build(),
                            System.currentTimeMillis() - message.getTimestamp());
                    }
                };
            client.publishAsync(arn, mapper.writeValueAsString(message), handler);
        } catch (final Exception e) {
            handleException(name, errorMessage, counterKey, message, e);
        }
    }
}
