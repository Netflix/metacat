package com.netflix.metacat.common.server.usermetadata;

/**
 * ParentChildRelMetadataConstants.
 *
 * @author yingjianw
 */
public final class ParentChildRelMetadataConstants {
    /**
     * During get and create, top level key specified in DefinitionMetadata that indicates the parent child infos.
     */
    public static final String PARENT_CHILD_RELINFO = "parentChildRelationInfo";
    /**
     * During create, nested level key specified in DefinitionMetadata['parentChildRelationInfo']
     * that indicate the parent table name.
     */
    public static final String PARENT_NAME = "parent_table_name";
    /**
     * During create, nested level key specified in DefinitionMetadata['parentChildRelationInfo']
     * that indicates the parent table uuid.
     */
    public static final String PARENT_UUID = "parent_table_uuid";
    /**
     * During create, nested level key specified in DefinitionMetadata['parentChildRelationInfo']
     * that indicates the child table uuid.
     */
    public static final String CHILD_UUID = "child_table_uuid";

    /**
     * During create, nested level key specified in DefinitionMetadata['parentChildRelationInfo']
     * that indicates relationType.
     */
    public static final String RELATION_TYPE = "relationType";

    /**
     * During get, the nested key specified in DefinitionMetadata[PARENTCHILDRELINFO] that indicates parent infos.
     */
    public static final String PARENT_INFOS = "parentInfos";

    /**
     * During get, the nested key specified in DefinitionMetadata[PARENTCHILDRELINFO] that indicates child infos.
     */
    public static final String CHILD_INFOS = "childInfos";

    /**
     * During get, the nested key specified in DefinitionMetadata[PARENTCHILDRELINFO]
     * that indicates if a table is parent.
     */
    public static final String IS_PARENT = "isParent";

    private ParentChildRelMetadataConstants() {

    }

}
