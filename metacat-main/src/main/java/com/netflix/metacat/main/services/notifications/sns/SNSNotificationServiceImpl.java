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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
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
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.main.services.notifications.NotificationService;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

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
    private final Retryer<PublishResult> retry;

    /**
     * Constructor.
     *
     * @param client            The SNS client to use to publish notifications
     * @param tableTopicArn     The topic to publish table related notifications to
     * @param partitionTopicArn The topic to publish partition related notifications to
     * @param mapper            The object mapper to use to convert objects to JSON strings
     * @param retry             The retry factory to use. Should already be configured
     */
    public SNSNotificationServiceImpl(
        @NotNull final AmazonSNSClient client,
        @NotNull @Size(min = 1) final String tableTopicArn,
        @NotNull @Size(min = 1) final String partitionTopicArn,
        @NotNull final ObjectMapper mapper,
        @NotNull final Retryer<PublishResult> retry
    ) {
        this.client = client;
        this.tableTopicArn = tableTopicArn;
        this.partitionTopicArn = partitionTopicArn;
        this.mapper = mapper;
        this.retry = retry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfPartitionAddition(@NotNull final MetacatSaveTablePartitionPostEvent event) {
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
        } catch (final ExecutionException | RetryException e) {
            log.error("Unable to publish partition creation notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.partitions.add.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfPartitionDeletion(@NotNull final MetacatDeleteTablePartitionPostEvent event) {
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
        } catch (final ExecutionException | RetryException e) {
            log.error("Unable to publish partition deletion notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.partitions.delete.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableCreation(@NotNull final MetacatCreateTablePostEvent event) {
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
        } catch (final ExecutionException | RetryException e) {
            log.error("Unable to publish create table notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.create.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableDeletion(@NotNull final MetacatDeleteTablePostEvent event) {
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
        } catch (final ExecutionException | RetryException e) {
            log.error("Unable to publish delete table notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.delete.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableRename(@NotNull final MetacatRenameTablePostEvent event) {
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
        } catch (final ExecutionException | IOException | RetryException e) {
            log.error("Unable to publish rename table notification", e);
            CounterWrapper.incrementCounter("metacat.notifications.sns.tables.rename.failed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOfTableUpdate(@NotNull final MetacatUpdateTablePostEvent event) {
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
        } catch (final IOException | ExecutionException  | RetryException e) {
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
    ) throws ExecutionException, RetryException {
        final PublishResult result = this.retry.call(
            () -> this.client.publish(arn, this.mapper.writeValueAsString(message))
        );
        log.debug("Successfully published message {} to topic {} with id {}", message, arn, result.getMessageId());
    }
}
