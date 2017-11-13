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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.BaseDto;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.server.connectors.exception.NotFoundException;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Metadata Service. This class includes any common services for the user metadata.
 * @author amajumdar
 */
@Slf4j
public class MetadataService {
    private final Config config;
    private final TableService tableService;
    private final PartitionService partitionService;
    private final UserMetadataService userMetadataService;
    private final TagService tagService;
    private final MetacatServiceHelper helper;
    private final Registry registry;

    /**
     * Constructor.
     * @param config                configuration
     * @param tableService          table service
     * @param partitionService      partition service
     * @param userMetadataService   usermetadata service
     * @param tagService            tag service
     * @param helper                service helper
     * @param registry              registry
     */
    public MetadataService(final Config config, final TableService tableService,
        final PartitionService partitionService,
        final UserMetadataService userMetadataService, final TagService tagService,
        final MetacatServiceHelper helper, final Registry registry) {
        this.config = config;
        this.tableService = tableService;
        this.partitionService = partitionService;
        this.userMetadataService = userMetadataService;
        this.tagService = tagService;
        this.helper = helper;
        this.registry = registry;
    }

    /**
     * Deletes all the data metadata marked for deletion.
     */
    public void cleanUpDeletedDataMetadata() {
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
            registry.counter(Metrics.CounterDeleteMetaData.getMetricName()).increment();
            log.warn("Failed deleting data metadata", e);
        }
        log.info("End deleting data metadata");
    }

    /**
     * Deletes definition metadata of tables/views/partitions that have been deleted already.
     */
    public void cleanUpObsoleteDefinitionMetadata() {
        log.info("Start deleting obsolete definition metadata");
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        List<DefinitionMetadataDto> dtos = null;
        int offset = 0;
        final int limit = 10000;
        final ThreadServiceManager threadServiceManager =
            new ThreadServiceManager(registry, 10, 20000, "definition");
        final ListeningExecutorService service = threadServiceManager.getExecutor();
        int totalDeletes = 0;
        while (offset == 0 || dtos.size() == limit) {
            dtos = userMetadataService.searchDefinitionMetadatas(null, null, null,
                "id", null, offset, limit);
            int deletes = 0;
            final List<ListenableFuture<Boolean>> futures = dtos.stream().map(dto ->
                service.submit(() -> deleteDefinitionMetadata(dto.getName(), false, metacatRequestContext)))
                .collect(Collectors.toList());
            try {
                deletes = Futures.transform(Futures.successfulAsList(futures),
                    (Function<List<Boolean>, Integer>) input ->
                        (int) (input != null ? input.stream().filter(b -> b != null && b).count() : 0)).get();
            } catch (Exception e) {
                log.warn("Failed deleting obsolete definition metadata for offset {}.", offset, e);
            }
            totalDeletes += deletes;
            offset += limit - deletes;
        }
        threadServiceManager.stop();
        log.info("End deleting obsolete definition metadata. Deleted {} number of definition metadatas", totalDeletes);
    }

    /**
     * Deletes definition metadata for the given <code>name</code>.
     * @param name qualified name
     * @param force If true, deletes the metadata without checking if the database/table/partition exists
     * @param metacatRequestContext request context
     * @return true if deleted
     */
    public boolean deleteDefinitionMetadata(final QualifiedName name, final boolean force,
        final MetacatRequestContext metacatRequestContext) {
        try {
            final MetacatService service = this.helper.getService(name);
            BaseDto dto = null;
            if (!force) {
                try {
                    dto = service.get(name);
                } catch (final NotFoundException ignored) {
                }
            }
            if ((force || dto == null) && !"rds".equalsIgnoreCase(name.getCatalogName())) {
                if (dto != null) {
                    this.helper.postPreUpdateEvent(name, metacatRequestContext, dto);
                } else {
                    this.helper.postPreDeleteEvent(name, metacatRequestContext);
                }
                this.userMetadataService.deleteDefinitionMetadatas(Lists.newArrayList(name));
                this.tagService.delete(name, false);
                log.info("Deleted obsolete definition metadata for {}", name);
                if (dto != null) {
                    final BaseDto newDto = service.get(name);
                    this.helper.postPostUpdateEvent(name, metacatRequestContext, dto, newDto);
                } else {
                    this.helper.postPostDeleteEvent(name, metacatRequestContext);
                }
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed deleting definition metadata for name {}.", name, e);
            throw e;
        }
        return false;
    }

    /**
     * Deletes tags for deleted tables.
     */
    public void cleanUpObsoleteTags() {
        log.info("Start deleting obsolete tags");
        final List<QualifiedName> names = tagService.list(null, null, null, null, null);
        names.forEach(name -> {
            if (!name.isPartitionDefinition() && !name.isViewDefinition() && name.isTableDefinition()
                && !tableService.exists(name)) {
                this.tagService.delete(name, false);
                log.info("Deleted obsolete tag for {}", name);
            }
        });
        log.info("End deleting obsolete tags");
    }
}
