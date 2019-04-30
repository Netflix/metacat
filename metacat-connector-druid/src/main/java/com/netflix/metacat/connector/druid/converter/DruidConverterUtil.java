/*
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
 */

package com.netflix.metacat.connector.druid.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.connector.druid.DruidConfigConstants;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Druid Converter Util.
 *
 * @author zhenl
 * @since 1.2.0
 */
public final class DruidConverterUtil {

    private DruidConverterUtil() {
    }

    /**
     * get segment.
     *
     * @param node object node
     * @return segment object
     */
    public static DataSource getDatasourceFromAllSegmentJsonObject(final ObjectNode node) {
        final Instant createTime = Instant.parse(
            node.get(DruidConfigConstants.PROPERTIES)
                .get(DruidConfigConstants.CREATED).asText());
        final String name = node.get(DruidConfigConstants.NAME).asText();
        final List<Segment> segmentList = new ArrayList<>();
        for (JsonNode segNode : node.get(DruidConfigConstants.SEGMENTS)) {
            final Segment segment = getSegmentFromJsonNode(segNode.deepCopy());
            segmentList.add(segment);
        }
        Collections.sort(segmentList);
        return new DataSource(name, createTime, segmentList);
    }

    /**
     * get segment.
     *
     * @param node object node
     * @return segment object
     */
    public static DataSource getDatasourceFromLatestSegmentJsonObject(final ObjectNode node) {
        final Segment segment = getSegmentFromJsonNode(node);
        return new DataSource(segment.getName(), segment.getInterval().getStart(), Collections.singletonList(segment));
    }

    /**
     * get segment count.
     *
     * @param node object node
     * @return segment count
     */
    public static int getSegmentCount(final ObjectNode node) {
        return node.get(DruidConfigConstants.SEGMENTS).size();
    }

    private static String getUriFromKey(final String bucket, final String key) {
        return bucket + "/" + key.substring(0, key.lastIndexOf("/"));
    }

    private static Segment getSegmentFromJsonNode(final ObjectNode node) {
        final String name = node.get(DruidConfigConstants.DATA_SOURCE).asText();
        final String[] intervalStr = node.get(DruidConfigConstants.INTERVAL).asText().split("/");
        final Interval interval = new Interval(Instant.parse(intervalStr[0]),
            Instant.parse(intervalStr[1]));
        final JsonNode loadspecNode = node.get(DruidConfigConstants.LOADSPEC);
        final JsonNode loadspecNodeBucket = loadspecNode.get(DruidConfigConstants.LOADSPEC_BUCKET);
        final String bucket = loadspecNodeBucket != null
            ? loadspecNode.get(DruidConfigConstants.LOADSPEC_BUCKET).asText() : "";
        final JsonNode loadspecNodeKey = loadspecNode.get(DruidConfigConstants.LOADSPEC_KEY);
        final List<String> keys = loadspecNodeKey != null
            ? Arrays.asList(loadspecNode.get(DruidConfigConstants.LOADSPEC_KEY).asText().split(","))
            : new ArrayList<>();
        final LoadSpec loadSpec = new LoadSpec(loadspecNode.get(DruidConfigConstants.LOADSPEC_TYPE).asText(),
            bucket, keys, StringUtils.isEmpty(bucket) || keys.size() == 0
            ? "" : getUriFromKey(bucket, keys.get(0))
        );
        final String dimensions = node.get(DruidConfigConstants.DIMENSIONS).asText();
        final String metric = node.get(DruidConfigConstants.METRICS).asText();
        return new Segment(name, interval, dimensions, metric, loadSpec);
    }
}
