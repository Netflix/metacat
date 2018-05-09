/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.main.services.notifications.sns;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.notifications.sns.payloads.TablePartitionsUpdatePayload;
import com.netflix.metacat.common.server.events.MetacatEvent;
import com.netflix.metacat.common.server.partition.util.PartitionUtil;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import lombok.extern.slf4j.Slf4j;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The util class for SNS Notification service.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Slf4j
public final class SNSNotificationServiceUtil {
    private static final String PARTITION_COLUMN_DATA_TYPE_PATH = "/data_dependency/partition_column_date_type";
    private static final String DELETION_COLUMN_PATH = "/data_hygiene/delete_column";
    private static final Set<String> PST_TIME = new HashSet<String>(Arrays.asList("region", "pacific"));
    //Timestamp in seconds: 1522257960 or 1367992474.293378
    //Timestamp in milliseconds: 1522257960000 or 1367992474000.293378
    //ISO basic date format: 20180101
    private static final Pattern TIMESTAMP_FORMAT = Pattern.compile("^(?<time>\\d{10})(?:\\d{3})?(?:\\.\\d+)?$");
    private static final Pattern ISO_BASIC = Pattern.compile("^\\d{8}$");
    private UserMetadataService userMetadataService;
    private final DateFormat simpleDateFormatRegional = new SimpleDateFormat("yyyyMMdd");
    private final DateFormat simpleDateFormatUTC = new SimpleDateFormat("yyyyMMdd");

    /**
     * SNS Notification Service Util constructor.
     *
     * @param userMetadataService user metadata service
     */
    public SNSNotificationServiceUtil(
        final UserMetadataService userMetadataService
    ) {
        this.userMetadataService = userMetadataService;
        this.simpleDateFormatRegional.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        this.simpleDateFormatUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * create table partition add payload.
     * The logic below primarily is for calculating the latest deletion column value in a batch of
     * partitions. The latest delete column value:
     * (1) valid timestamp/date format
     * (2) the latest timestamp from the delete column
     * (3) the timestamp must be less or equal to today ( utc now )
     *
     * @param partitionDtos partition DTOs
     * @param event         Metacat event
     * @return TablePartitionsUpdatePayload
     */
    public TablePartitionsUpdatePayload createTablePartitionsUpdatePayload(
        final List<PartitionDto> partitionDtos,
        final MetacatEvent event) {
        final List<String> deleteColumnValues;
        String latestDeleteColumnValue = null;
        final Optional<ObjectNode> objectNode = this.userMetadataService.getDefinitionMetadata(
            QualifiedName.ofTable(event.getName().getCatalogName(), event.getName().getDatabaseName(),
                event.getName().getTableName()));
        String message = SNSNotificationPartitionAddMsg.ALL_FUTURE_PARTITION_KEYS.name();
        if (objectNode.isPresent()
            && !objectNode.get().at(DELETION_COLUMN_PATH).isMissingNode()
            && !objectNode.get().at(PARTITION_COLUMN_DATA_TYPE_PATH).isMissingNode()) {
            final String deleteColumn = objectNode.get().at(DELETION_COLUMN_PATH).textValue();
            deleteColumnValues = getSortedDeletionPartitionKeys(partitionDtos, deleteColumn);
            //using utc now as today
            final long nowSecond = Instant.now().getEpochSecond();
            final boolean regional = PST_TIME.contains(
                objectNode.get().at(PARTITION_COLUMN_DATA_TYPE_PATH).textValue());
            //convert the value to utc then compare
            for (String val : deleteColumnValues) {
                try {
                    final Long timestamp = getTimeStamp(val, regional);
                    if (timestamp <= nowSecond) {
                        latestDeleteColumnValue = deleteColumn + "=" + val; //the delete column with value
                        message = SNSNotificationPartitionAddMsg.ATTACHED_VALID_PARITITION_KEY.name();
                        break;
                    }
                } catch (ParseException ex) {
                    message = SNSNotificationPartitionAddMsg.INVALID_PARTITION_KEY_FORMAT.name();
                    log.debug("Failure of getting latest key due to invalid timestamp format {} {}:{}",
                        event.getName().getTableName(), deleteColumn, val);
                    break;
                }
            }
        } else {
            message = SNSNotificationPartitionAddMsg.MISSING_METADATA_INFO_FOR_PARTITION_KEY.name();
        }
        return new TablePartitionsUpdatePayload(
            latestDeleteColumnValue,
            partitionDtos.size(),
            0,
            message
        );
    }

    /**
     * get descending order deletion column value.
     *
     * @param partitionDtos partition DTOs
     * @param deleteColumn  delete column name
     * @return descending order deletion column
     */
    public List<String> getSortedDeletionPartitionKeys(final List<PartitionDto> partitionDtos,
                                                       final String deleteColumn) {
        final List<String> ret = partitionDtos.stream().map(
            x -> PartitionUtil.getPartitionKeyValues(x.getName().toString()).get(deleteColumn)
        ).sorted().collect(Collectors.toList());
        Collections.reverse(ret);
        return ret;
    }

    /**
     * convert string to time stamp.
     * Three formats are accepted for now, which are basic standard ISO format and epoch timestamp format.
     *
     * @param timeStr  time in string
     * @param regional in pst
     * @return timestamp
     * @throws ParseException parsing error
     */
    public Long getTimeStamp(final String timeStr, final boolean regional) throws ParseException {
        final Matcher m = TIMESTAMP_FORMAT.matcher(timeStr);
        if (m.find()) {
            return Long.parseLong(m.group("time"));
        }
        if (ISO_BASIC.matcher(timeStr).matches()) {
            if (regional) {
                return this.simpleDateFormatRegional.parse(timeStr).toInstant().getEpochSecond();
            } else {
                return this.simpleDateFormatUTC.parse(timeStr).toInstant().getEpochSecond();
            }
        }
        throw new ParseException("Unknown format", 0);
    }

}
