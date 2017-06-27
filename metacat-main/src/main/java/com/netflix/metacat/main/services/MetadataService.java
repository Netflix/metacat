/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.main.services;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.spectator.api.Registry;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Metadata Service. This class includes any common services for the user metadata.
 * Created by amajumdar on 9/26/16.
 */
@Slf4j
@AllArgsConstructor
public class MetadataService {
    private final Config config;
    private final TableService tableService;
    private final PartitionService partitionService;
    private final UserMetadataService userMetadataService;
    private final Registry registry;

    /**
     * Deletes all the data metadata marked for deletion.
     */
    public void processDeletedDataMetadata() {
        // Get the data metadata that were marked deleted a number of days back
        // Check if the uri is being used
        // If uri is not used then delete the entry from data_metadata
        log.info("Start deleting data metadata");
        try {
            final DateTime priorTo = DateTime.now().minusDays(config.getDataMetadataDeleteMarkerLifetimeInDays());
            final int limit = 100000;
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            while (true) {
                final List<String> urisToDelete =
                    userMetadataService.getDeletedDataMetadataUris(priorTo.toDate(), 0, limit);
                log.info("Count of deleted marked data metadata: {}", urisToDelete.size());
                if (urisToDelete.size() > 0) {
                    final List<String> uris = urisToDelete.parallelStream().filter(uri -> !uri.contains("="))
                        .map(userMetadataService::getDescendantDataUris)
                        .flatMap(Collection::stream).collect(Collectors.toList());
                    uris.addAll(urisToDelete);
                    log.info("Count of deleted marked data metadata (including descendants) : {}", uris.size());
                    final List<List<String>> subListsUris = Lists.partition(uris, 1000);
                    subListsUris.parallelStream().forEach(subUris -> {
                        MetacatContextManager.setContext(metacatRequestContext);
                        final Map<String, List<QualifiedName>> uriPartitionQualifiedNames = partitionService
                            .getQualifiedNames(subUris, false);
                        final Map<String, List<QualifiedName>> uriTableQualifiedNames = tableService
                            .getQualifiedNames(subUris, false);
                        final Map<String, List<QualifiedName>> uriQualifiedNames =
                            Stream.concat(uriPartitionQualifiedNames.entrySet().stream(),
                                uriTableQualifiedNames.entrySet().stream())
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> {
                                    final List<QualifiedName> subNames = Lists.newArrayList(a);
                                    subNames.addAll(b);
                                    return subNames;
                                }));
                        final List<String> canDeleteMetadataForUris = subUris.parallelStream()
                            .filter(s -> !Strings.isNullOrEmpty(s))
                            .filter(s -> uriQualifiedNames.get(s) == null || uriQualifiedNames.get(s).size() == 0)
                            .collect(Collectors.toList());
                        log.info("Start deleting data metadata: {}", canDeleteMetadataForUris.size());
                        userMetadataService.deleteDataMetadatas(canDeleteMetadataForUris);
                        userMetadataService.deleteDataMetadataDeletes(subUris);
                        MetacatContextManager.removeContext();
                    });
                }
                if (urisToDelete.size() < limit) {
                    break;
                }
            }
        } catch (Exception e) {
            registry.counter(Metrics.CounterDeleteMetaData.name()).increment();
            log.warn("Failed deleting data metadata", e);
        }
        log.info("End deleting data metadata");
    }
}
