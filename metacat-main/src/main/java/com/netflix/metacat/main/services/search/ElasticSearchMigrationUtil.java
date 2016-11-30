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

package com.netflix.metacat.main.services.search;

import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.Config;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

/**
 * Created by zhenli on 11/29/16.
 */
public class ElasticSearchMigrationUtil extends ElasticSearchUtil {
    private final String esMergeIndex;
    private final String[] esWriteIndices;
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchMigrationUtil.class);

    @Inject
    public ElasticSearchMigrationUtil(@Nullable Client client, Config config, MetacatJson metacatJson) {
        super(client, config, metacatJson);
        esMergeIndex = config.getMergeEsIndex();
        esWriteIndices = new String[] {esIndex, esMergeIndex};
    }

    /**
     * Save of a single entity to multiple indices
     * @param type index type
     * @param id id of the entity
     * @param body source string of the entity
     */
    @Override
    public void save(String type, String id, String body) {
        for ( String _index : esWriteIndices ) {
            _saveToIndex(type, id, body, _index);
        }
    }

    /**
     * Bulk save of the entities
     * @param type index type
     * @param docs metacat documents
     */
    @Override
    public void save(String type, List<ElasticSearchDoc> docs) {
        if( docs != null && !docs.isEmpty()) {
            List<List<ElasticSearchDoc>> partitionedDocs = Lists.partition(docs, 100);
            partitionedDocs.forEach(subDocs -> _bulkSaveToIndex(type, subDocs, esWriteIndices));
        }
    }

    /**
     * Marks the document as deleted from multiple indices
     * @param type index type
     * @param id entity id
     * @param metacatContext context containing the user name
     */
    @Override
    public void softDelete(String type, String id, MetacatRequestContext metacatContext) {
        //mark the records as soft deleted in the original index
        super.softDelete(type, id, metacatContext);
        _copyDataToMergeIndex(type, Collections.singletonList(id));
    }

    @Override
    protected void _softDelete(String type, List<String> ids, MetacatRequestContext metacatContext) {
        super._softDelete(type, ids, metacatContext);
        _copyDataToMergeIndex(type, ids);
    }

    /*
     * Read the document from source index
     * Reindex it to the merge index
     */
    private void _copyDataToMergeIndex(String type, List<String> ids) {
        List<ElasticSearchDoc> docs = new ArrayList<>();
        ids.forEach(id->{
                ElasticSearchDoc doc = super.get(type, id);
                if ( doc != null ) docs.add(doc);
            });
        _bulkSaveToIndex(type, docs, esMergeIndex);
    }
}
