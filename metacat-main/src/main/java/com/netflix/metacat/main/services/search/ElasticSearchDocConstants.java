/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.main.services.search;

/**
 * ElasticSearchDocConstants.
 *
 * @author zhenl
 */
final class ElasticSearchDocConstants {
    /**
     * DEFINITION_METADATA.
     */
    static final String DEFINITION_METADATA = "definitionMetadata";

    /**
     * DEFINITION_METADATA_OWNER.
     */
    static final String DEFINITION_METADATA_OWNER = "owner";

    /**
     * DEFINITION_METADATA_TAGS.
     */
    static final String DEFINITION_METADATA_TAGS = "tags";
    /**
     * DEFINITION_METADATA_DATA_HYGIENE.
     */
    static final String DEFINITION_METADATA_DATA_HYGIENE = "data_hygiene";

    /**
     * DEFINITION_METADATA_LIFETIME.
     */
    static final String DEFINITION_METADATA_LIFETIME = "lifetime";
    /**
     * DEFINITION_METADATA_EXTENDED_SCHEMA.
     */
    static final String DEFINITION_METADATA_EXTENDED_SCHEMA = "extendedSchema";
    /**
     * DEFINITION_METADATA_DATA_DEPENDENCY.
     */
    static final String DEFINITION_METADATA_DATA_DEPENDENCY = "data_dependency";
    /**
     * DEFINITION_METADATA_TABLE_COST.
     */
    static final String DEFINITION_METADATA_TABLE_COST = "table_cost";
    /**
     * DEFINITION_METADATA_LIFECYCLE.
     */
    static final String DEFINITION_METADATA_LIFECYCLE = "lifecycle";
    /**
     * DEFINITION_METADATA_AUDIENCE.
     */
    static final String DEFINITION_METADATA_AUDIENCE = "audience";
    /**
     * DEFINITION_METADATA_MODEL.
     */
    static final String DEFINITION_METADATA_MODEL = "model";
    //TODO: remove after the data are fixed and copied to subjectAreas
    /**
     * DEFINITION_METADATA_SUBJECT_AREA.
     */
    static final String DEFINITION_METADATA_SUBJECT_AREA = "subject_area";
    /**
     * DEFINITION_METADATA_SUBJECT_AREAS.
     */
    static final String DEFINITION_METADATA_SUBJECT_AREAS = "subjectAreas";
    /**
     * DEFINITION_METADATA_DATA_CATEGORY.
     */
    static final String DEFINITION_METADATA_DATA_CATEGORY = "data_category";
    /**
     * DEFINITION_METADATA_JOB.
     */
    static final String DEFINITION_METADATA_JOB = "job";

    /**
     * DEFINITION_METADATA_TABLE_DESCRIPTION.
     */
    static final String DEFINITION_METADATA_TABLE_DESCRIPTION = "table_description";

    /**
     * DEFINITION_DATA_MANAGEMENT.
     */
    static final String  DEFINITION_DATA_MANAGEMENT = "data_management";

    private ElasticSearchDocConstants() {

    }
}
