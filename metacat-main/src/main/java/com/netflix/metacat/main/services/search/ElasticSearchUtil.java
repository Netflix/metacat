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
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Utility class for index, update, delete metacat doc from elastic search.
 */

public interface ElasticSearchUtil {
    /**
     * Delete the records for the given type.
     *
     * @param metacatRequestContext context
     * @param type                  doc type
     * @param softDelete            if true, marks the doc for deletion
     */
    void delete(MetacatRequestContext metacatRequestContext, String type,
                boolean softDelete);

    /**
     * Delete index documents.
     *
     * @param type index type
     * @param ids  entity ids
     */
    void delete(String type, List<String> ids);

    /**
     * Delete index document.
     *
     * @param type index type
     * @param id   entity id
     */
    void delete(String type, String id);

    /**
     * Gets the document for the given type and id.
     *
     * @param type doc type
     * @param id   doc id
     * @return doc
     */
    ElasticSearchDoc get(String type, String id);

    /**
     * Gets the document for the given type and id.
     *
     * @param type  doc type
     * @param id    doc id
     * @param index the es index
     * @return doc
     */
    ElasticSearchDoc get(String type, String id, String index);

    /**
     * List of names.
     *
     * @param type          type
     * @param qualifiedName name
     * @return list of names
     */
    List<String> getIdsByQualifiedName(String type, QualifiedName qualifiedName);

    /**
     * Search the names by names and by the given marker.
     *
     * @param type                  type
     * @param qualifiedNames        names
     * @param marker                marker
     * @param excludeQualifiedNames exclude names
     * @param valueType             dto type
     * @param <T>                   dto type
     * @return dto
     */
    <T> List<T> getQualifiedNamesByMarkerByNames(String type, List<QualifiedName> qualifiedNames,
                                                 Instant marker,
                                                 List<QualifiedName> excludeQualifiedNames,
                                                 Class<T> valueType);

    /**
     * List table names.
     *
     * @param type                  doc type
     * @param qualifiedNames        names
     * @param excludeQualifiedNames exclude names
     * @return list of table names
     */
    List<String> getTableIdsByCatalogs(String type, List<QualifiedName> qualifiedNames,
                                       List<QualifiedName> excludeQualifiedNames);

    /**
     * List table names by uri.
     *
     * @param type    doc type
     * @param dataUri uri
     * @return list of table names
     */
    List<String> getTableIdsByUri(String type, String dataUri);

    /**
     * Wrapper for logging the message in elastic search esIndex.
     *
     * @param method     method
     * @param type       type
     * @param name       name
     * @param data       data
     * @param logMessage message
     * @param ex         exception
     * @param error      is an error
     */
    void log(String method, String type, String name, @Nullable String data,
             String logMessage, @Nullable Exception ex, boolean error);

    /**
     * Elastic search index refresh.
     */
    void refresh();

    /**
     * Bulk save of the entities.
     *
     * @param type index type
     * @param docs metacat documents
     */
    void save(String type, List<ElasticSearchDoc> docs);

    /**
     * Save of a single entity.
     *
     * @param type index type
     * @param id   id of the entity
     * @param doc metacat documents
     */
    void save(String type, String id, ElasticSearchDoc doc);

    /**
     * Full text search.
     *
     * @param searchString search text
     * @return list of table info
     */
    List<TableDto> simpleSearch(String searchString);

    /**
     * Marks the documents as deleted.
     *
     * @param type                  index type
     * @param ids                   list of entity ids
     * @param metacatRequestContext context containing the user name
     */
    void softDelete(String type, List<String> ids,
                    MetacatRequestContext metacatRequestContext);


    /**
     * Marks the document as deleted.
     *
     * @param type                  index type
     * @param id                    entity id
     * @param metacatRequestContext context containing the user name
     */
    void softDelete(String type, String id, MetacatRequestContext metacatRequestContext);

    /**
     * Creates JSON from elasticSearchdoc object.
     *
     * @param elasticSearchDoc elastic search doc.
     * @return Json String
     */
    String toJsonString(ElasticSearchDoc elasticSearchDoc);

    /**
     * Updates the documents with partial updates with the given fields.
     *
     * @param type                  index type
     * @param ids                   list of entity ids
     * @param node    Object node to update the doc
     */
    void updates(String type, List<String> ids, ObjectNode node);
}
