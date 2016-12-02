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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.Config;
import org.elasticsearch.client.Client;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * Class for migrating the elastic search index to a new one. It overrides the save, update, and softdelete
 * to populate the doc to both the original and new indices
 */
public class ElasticSearchMigrationUtil extends ElasticSearchUtil {
    private final String esMergeIndex;
    private final String[] esWriteIndices;

    /**
     * Constructor.
     * @param client elastic search client
     * @param config config
     * @param metacatJson json utility
     */
    @Inject
    public ElasticSearchMigrationUtil(
        @Nullable
        final Client client,
        final Config config,
        final MetacatJson metacatJson) {
        super(client, config, metacatJson);
        esMergeIndex = config.getMergeEsIndex();
        esWriteIndices = new String[] {esIndex, esMergeIndex};
    }

    /**
     * Save of a single entity to default index and merge index.
     * @param type index type
     * @param id id of the entity
     * @param body source string of the entity
     */
    @Override
    public void save(final String type, final String id, final String body) {
        for (String index : esWriteIndices) {
            saveToIndex(type, id, body, index);
        }
    }

    /**
     * Bulk save of the docs to default index and merge index.
     * @param type index type
     * @param docs metacat documents
     */
    @Override
    public void save(final String type, final List<ElasticSearchDoc> docs) {
        if (docs != null && !docs.isEmpty()) {
            final List<List<ElasticSearchDoc>> partitionedDocs = Lists.partition(docs, 100);
            partitionedDocs.forEach(subDocs -> bulkSaveToIndex(type, subDocs, esWriteIndices));
        }
    }

    /**
     * Marks the document as deleted from default index and copy the marked docs to merge index.
     * @param type index type
     * @param id doc id
     * @param metacatContext context containing the user name
     */
    @Override
    public void softDelete(final String type, final String id, final MetacatRequestContext metacatContext) {
        //mark the records as soft deleted in the original index
        super.softDelete(type, id, metacatContext);
        copyDataToMergeIndex(type, Collections.singletonList(id));
    }

    /**
     * Marks the document as deleted from default index and copy the marked docs to merge index.
     * @param type index type
     * @param ids list of doc id
     * @param metacatContext context containing the user name
     */
    @Override
    protected void softDeleteDoc(final String type, final List<String> ids,
                                 final MetacatRequestContext metacatContext) {
        super.softDeleteDoc(type, ids, metacatContext);
        copyDataToMergeIndex(type, ids);
    }

    /**
     * Updates the documents with partial updates with the given fields in default index.
     * copy the updated doc to merge index
     * @param type index type
     * @param ids list of doc ids
     * @param metacatContext context containing the user name
     * @param node json that represents the document source
     */
    @Override
    public void updates(final String type, final List<String> ids,
                        final MetacatRequestContext metacatContext, final ObjectNode node) {
        super.updates(type, ids, metacatContext, node);
        copyDataToMergeIndex(type, ids);
    }

    /*
     * Read the documents from source index copy to the merge index
     * @param type index type
     * @param ids list of doc ids
     */
    private void copyDataToMergeIndex(final String type, final List<String> ids) {
        final List<ElasticSearchDoc> docs = new ArrayList<>();
        ids.forEach(id-> {
                final ElasticSearchDoc doc = super.get(type, id);
                if (doc != null) {
                    docs.add(doc);
                }
            });
        bulkSaveToIndex(type, docs, esMergeIndex);
    }
}
