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

import java.util.List;

/**
 * Utility class for index, update, delete metacat doc from elastic search.
 */

public interface ElasticSearchUtil {
    /**
     * Delete the records for the given type.
     * @param metacatRequestContext context
     * @param type doc type
     * @param softDelete if true, marks the doc for deletion
     */
    void delete(final MetacatRequestContext metacatRequestContext, final String type,
                       final boolean softDelete);

    /**
     * Delete index documents.
     * @param type index type
     * @param ids entity ids
     */
    void delete(final String type, final List<String> ids);

    /**
     * Delete index document.
     * @param type index type
     * @param id entity id
     */
    void delete(final String type, final String id);

    /**
     * Gets the document for the given type and id.
     * @param type doc type
     * @param id doc id
     * @return doc
     */
    ElasticSearchDoc get(final String type, final String id);

    /**
     * List of names.
     * @param type type
     * @param qualifiedName name
     * @return list of names
     */
    List<String> getIdsByQualifiedName(final String type, final QualifiedName qualifiedName);

    /**
     * Search the names by names and by the given marker.
     * @param type type
     * @param qualifiedNames names
     * @param marker marker
     * @param excludeQualifiedNames exclude names
     * @param valueType dto type
     * @param <T> dto type
     * @return dto
     */
    <T> List<T> getQualifiedNamesByMarkerByNames(final String type, final List<QualifiedName> qualifiedNames,
                                                        final Instant marker,
                                                        final List<QualifiedName> excludeQualifiedNames,
                                                        final Class<T> valueType);

    /**
     * List table names.
     * @param type doc type
     * @param qualifiedNames names
     * @param excludeQualifiedNames exclude names
     * @return list of table names
     */
    List<String> getTableIdsByCatalogs(final String type, final List<QualifiedName> qualifiedNames,
                                              final List<QualifiedName> excludeQualifiedNames);

    /**
     * List table names by uri.
     * @param type doc type
     * @param dataUri uri
     * @return list of table names
     */
    List<String> getTableIdsByUri(final String type, final String dataUri);

    /**
     * Wrapper for logging the message in elastic search esIndex.
     * @param method method
     * @param type type
     * @param name name
     * @param data data
     * @param logMessage message
     * @param ex exception
     * @param error is an error
     */
    void log(final String method, final String type, final String name, final String data,
                    final String logMessage, final Exception ex, final boolean error);

    /**
     * Elastic search index refresh.
     */
    void refresh();

    /**
     * Bulk save of the entities.
     * @param type index type
     * @param docs metacat documents
     */
    void save(final String type, final List<ElasticSearchDoc> docs);

    /**
     * Save of a single entity.
     * @param type index type
     * @param id id of the entity
     * @param body source string of the entity
     */
    void save(final String type, final String id, final String body);

    /**
     * Full text search.
     * @param searchString search text
     * @return list of table info
     */
    List<TableDto> simpleSearch(final String searchString);

    /**
     * Marks the documents as deleted.
     * @param type index type
     * @param ids list of entity ids
     * @param metacatRequestContext context containing the user name
     */
    void softDelete(final String type, final List<String> ids,
                           final MetacatRequestContext metacatRequestContext);


    /**
     * Marks the document as deleted.
     * @param type index type
     * @param id entity id
     * @param metacatRequestContext context containing the user name
     */
    void softDelete(final String type, final String id, final MetacatRequestContext metacatRequestContext);

    /**
     * Creates JSON from search doc.
     * @param id doc id
     * @param dto dto
     * @param context context
     * @param isDeleted true if it has to be mark deleted
     * @return doc
     */
    String toJsonString(final String id, final Object dto, final MetacatRequestContext context,
                               final boolean isDeleted);

    /**
     * Updates the documents with partial updates with the given fields.
     * @param type index type
     * @param ids list of entity ids
     * @param metacatRequestContext context containing the user name
     * @param node json that represents the document source
     */
    void updates(final String type, final List<String> ids,
                        final MetacatRequestContext metacatRequestContext, final ObjectNode node);
}
