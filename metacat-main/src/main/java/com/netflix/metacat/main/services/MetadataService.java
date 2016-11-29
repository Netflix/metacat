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
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.MetacatContextManager;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by amajumdar on 9/26/16.
 */
public class MetadataService {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);
    @Inject
    UserMetadataService userMetadataService;
    @Inject
    Config config;
    @Inject
    PartitionService partitionService;

    public void processDeletedDataMetadata(){
        // Get the data metadata that were marked deleted a number of days back
        // Check if the uri is being used
        // If uri is not used then delete the entry from data_metadata
        log.info("Start deleting data metadata");
        try {
            final DateTime priorTo = DateTime.now().minusDays(config.getDataMetadataDeleteMarkerLifetimeInDays());
            final int limit = 100000;
            MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            while(true) {
                List<String> urisToDelete = userMetadataService.getDeletedDataMetadataUris(priorTo.toDate(), 0, limit);
                log.info("Count of deleted marked data metadata: {}", urisToDelete.size());
                if( urisToDelete.size() > 0) {
                    List<String> uris = urisToDelete.parallelStream().filter(uri -> !uri.contains("="))
                            .map(uri -> userMetadataService.getDescendantDataUris(uri))
                            .flatMap(Collection::stream).collect(Collectors.toList());
                    uris.addAll(urisToDelete);
                    log.info("Count of deleted marked data metadata (including descendants) : {}", uris.size());
                    List<List<String>> subListsUris = Lists.partition(uris, 1000);
                    subListsUris.parallelStream().forEach(subUris -> {
                        MetacatContextManager.setContext(metacatRequestContext);
                        Map<String, List<QualifiedName>> uriQualifiedNames = partitionService.getQualifiedNames(subUris, false);
                        List<String> canDeleteMetadataForUris = subUris.parallelStream()
                                .filter(s -> !Strings.isNullOrEmpty(s))
                                .filter(s -> uriQualifiedNames.get(s) == null || uriQualifiedNames.get(s).size() == 0)
                                .collect(Collectors.toList());
                        log.info("Start deleting data metadata: {}", canDeleteMetadataForUris.size());
                        userMetadataService.deleteDataMetadatas(canDeleteMetadataForUris);
                        MetacatContextManager.removeContext();
                    });
                }
                if( urisToDelete.size() < limit){
                    break;
                }
            }
        } catch(Exception e){
            CounterWrapper.incrementCounter("dse.metacat.processDeletedDataMetadata");
            log.warn("Failed deleting data metadata", e);
        }
        log.info("End deleting data metadata");
    }
}
