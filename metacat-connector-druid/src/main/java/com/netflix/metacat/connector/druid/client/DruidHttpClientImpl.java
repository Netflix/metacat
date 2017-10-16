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

package com.netflix.metacat.connector.druid.client;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.connector.druid.DruidConfigConstants;
import com.netflix.metacat.connector.druid.MetacatDruidClient;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * DruidHttpClientImpl.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Slf4j
public class DruidHttpClientImpl implements MetacatDruidClient {
    private String druidURI;
    private final RestTemplate restTemplate;
    private final MetacatJsonLocator jsonLocator = new MetacatJsonLocator();

    /**
     * Constructor.
     *
     * @param connectorContext connector context
     * @param restTemplate     rest template
     */
    public DruidHttpClientImpl(final ConnectorContext connectorContext,
                               final RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
        final Map<String, String> config = connectorContext.getConfiguration();
        final String coordinatorUri = config.get(DruidConfigConstants.DRUID_COORDINATOR_URI);
        if (coordinatorUri == null) {
            throw new MetacatException("Druid cluster ending point not provided.");
        }
        try {
            new URI(coordinatorUri);
        } catch (URISyntaxException exception) {
            throw new MetacatException("Druid ending point invalid");
        }
        this.druidURI = coordinatorUri;
        log.info("druid server uri={}", this.druidURI);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllDataSources() {
        final JSONArray arr = new JSONArray(restTemplate.getForObject(druidURI, String.class));
        return IntStream.range(0, arr.length()).mapToObj(i -> arr.get(i).toString()).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public ObjectNode getAllDataByName(final String dataSourceName) {
        final String result = restTemplate.getForObject(
            druidURI + "/{datasoureName}?full", String.class, dataSourceName);
        return jsonLocator.parseJsonObject(result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public ObjectNode getLatestDataByName(final String dataSourceName) {
        String url = String.format(druidURI + "/%s/segments", dataSourceName);
        String result = restTemplate.getForObject(url, String.class);
        final String latestSegment = DruidHttpClientUtil.getLatestSegment(result);
        log.debug("Get the latest segment {}", latestSegment);
        url = String.format(druidURI + "/%s/segments/%s", dataSourceName, latestSegment);
        result = restTemplate.getForObject(url, String.class);
        return jsonLocator.parseJsonObject(result);
    }

}
