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
import com.amazonaws.services.sns.model.PublishResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
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
import com.netflix.metacat.common.server.monitoring.CounterWrapper;
import com.netflix.metacat.main.services.notifications.NotificationService;
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

    private final AmazonSNSClient client;
    private final String tableTopicArn;
    private final String partitionTopicArn;
    private final ObjectMapper mapper;

    /**
     * Constructor.
     *
     * @param client            The SNS client to use to publish notifications
     * @param tableTopicArn     The topic to publish table related notifications to
     * @param partitionTopicArn The topic to publish partition related notifications to
     * @param mapper            The object mapper to use to convert objects to JSON strings
     */
    public SNSNotificationServiceImpl(
        @Nonnull final AmazonSNSClient client,
        @Nonnull @Size(min = 1) final String tableTopicArn,
        @Nonnull @Size(min = 1) final String partitionTopicArn,
        @Nonnull final ObjectMapper mapper
    ) {
        this.client = client;
        this.tableTopicArn = tableTopicArn;
        this.partitionTopicArn = partitionTopicArn;
        this.mapper = mapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfPartitionAddition(@Nonnull final MetacatSaveTablePartitionPostEvent event) {
        log.debug("Received SaveTablePartitionPostEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.partitions.add");
        try {
            final String name = event.getName().toString();
            final long timestamp = event.getRequestContext().getTimestamp();
            final String requestId = event.getRequestContext().getId();
            for (final PartitionDto partition : event.getPartitions()) {
                final AddPartitionMessage message = new AddPartitionMessage(
                    UUID.randomUUID().toString(),
                    timestamp,
                    requestId,
                    name,
                    partition
                );
                this.publishNotification(this.partitionTopicArn, message);
                log.debug("Published create partition message {} on {}", message, this.partitionTopicArn);
                CounterWrapper.incrementCounter("metacat.notifications.sns.partitions.add.succeeded");
            }

            // Publish a global message stating how many partitions were updated for the table to the table topic
            final UpdateTablePartitionsMessage message = new UpdateTablePartitionsMessage(
                UUID.randomUUID().toString(),
                timestamp,
                requestId,
                name,
                new TablePartitionsUpdatePayload(
                    event.getPartitions().size(),
                    0
                )
            );
            this.publishNotification(this.tableTopicArn, message);
            // TODO: In ideal world this this be an injected object to the class so we can mock for tests
            //       swap out implementations etc.
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.addPartitions.succeeded");
        } catch (final Exception e) {
            log.error("Unable to publish partition creation notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.partitions.add.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfPartitionDeletion(@Nonnull final MetacatDeleteTablePartitionPostEvent event) {
        log.debug("Received DeleteTablePartition event {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.partitions.delete");
        try {
            final String name = event.getName().toString();
            final long timestamp = event.getRequestContext().getTimestamp();
            final String requestId = event.getRequestContext().getId();
            for (final String partitionId : event.getPartitionIds()) {
                final DeletePartitionMessage message = new DeletePartitionMessage(
                    UUID.randomUUID().toString(),
                    timestamp,
                    requestId,
                    name,
                    partitionId
                );
                this.publishNotification(this.partitionTopicArn, message);
                CounterWrapper.incrementCounter("metacat.notifications.sns.partitions.delete.succeeded");
                log.debug("Published delete partition message {} on {}", message, this.partitionTopicArn);
            }

            // Publish a global message stating how many partitions were updated for the table to the table topic
            final UpdateTablePartitionsMessage message = new UpdateTablePartitionsMessage(
                UUID.randomUUID().toString(),
                timestamp,
                requestId,
                name,
                new TablePartitionsUpdatePayload(
                    0,
                    event.getPartitionIds().size()
                )
            );
            this.publishNotification(this.tableTopicArn, message);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.deletePartitions.succeeded");
        } catch (final Exception e) {
            log.error("Unable to publish partition deletion notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.partitions.delete.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableCreation(@Nonnull final MetacatCreateTablePostEvent event) {
        log.debug("Received CreateTableEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.tables.create");
        try {
            final CreateTableMessage message = new CreateTableMessage(
                UUID.randomUUID().toString(),
                event.getRequestContext().getTimestamp(),
                event.getRequestContext().getId(),
                event.getName().toString(),
                event.getTable()
            );
            this.publishNotification(this.tableTopicArn, message);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.create.succeeded");
        } catch (final Exception e) {
            log.error("Unable to publish create table notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.create.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableDeletion(@Nonnull final MetacatDeleteTablePostEvent event) {
        log.debug("Received DeleteTableEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.tables.delete");
        try {
            final DeleteTableMessage message = new DeleteTableMessage(
                UUID.randomUUID().toString(),
                event.getRequestContext().getTimestamp(),
                event.getRequestContext().getId(),
                event.getName().toString(),
                event.getTable()
            );
            this.publishNotification(this.tableTopicArn, message);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.delete.succeeded");
        } catch (final Exception e) {
            log.error("Unable to publish delete table notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.delete.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableRename(@Nonnull final MetacatRenameTablePostEvent event) {
        log.debug("Received RenameTableEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.tables.rename");
        try {
            final UpdateTableMessage message = this.createUpdateTableMessage(
                UUID.randomUUID().toString(),
                event.getRequestContext().getTimestamp(),
                event.getRequestContext().getId(),
                event.getName().toString(),
                event.getOldTable(),
                event.getCurrentTable()
            );
            this.publishNotification(this.tableTopicArn, message);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.rename.succeeded");
        } catch (final Exception e) {
            log.error("Unable to publish rename table notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.rename.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableUpdate(@Nonnull final MetacatUpdateTablePostEvent event) {
        log.debug("Received UpdateTableEvent {}", event);
        CounterWrapper.incrementCounter("metacat.notifications.sns.events.tables.update");
        try {
            final UpdateTableMessage message = this.createUpdateTableMessage(
                UUID.randomUUID().toString(),
                event.getRequestContext().getTimestamp(),
                event.getRequestContext().getId(),
                event.getName().toString(),
                event.getOldTable(),
                event.getCurrentTable()
            );
            this.publishNotification(this.tableTopicArn, message);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.update.succeeded");
        } catch (final Exception e) {
            log.error("Unable to publish update table notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.update.failed");
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
            new UpdatePayload<>(oldTable, patch, currentTable)
        );
    }

    private void publishNotification(
        final String arn,
        final SNSMessage<?> message
    ) throws JsonProcessingException {
        final PublishResult result = client.publish(arn, mapper.writeValueAsString(message));
        log.debug("Successfully published message {} to topic {} with id {}", message, arn, result.getMessageId());
    }
}
