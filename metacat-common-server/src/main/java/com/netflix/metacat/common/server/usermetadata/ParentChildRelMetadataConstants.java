package com.netflix.metacat.common.server.usermetadata;

/**
 * ParentChildRelMetadataConstants.
 *
 * @author yingjianw
 */
public final class ParentChildRelMetadataConstants {
    /**
     * During create, top level key specified in DefinitionMetadata that indicate the parent table name.
     */
    public static final String PARENTNAME = "root_table_name";
    /**
     * During create, top level key specified in DefinitionMetadata that indicates the parent table uuid.
     */
    public static final String PARENTUUID = "root_table_uuid";
    /**
     * During create, top level key specified in DefinitionMetadata that indicates the child table uuid.
     */
    public static final String CHILDUUID = "child_table_uuid";

    /**
     * During create, top level key specified in DefinitionMetadata that indicates relationType.
     */
    public static final String RELATIONTYPE = "relationType";

    /**
     * During get, top level key specified in DefinitionMetadata that indicates the parent child infos.
     */
    public static final String PARENTCHILDRELINFO = "parentChildRelationInfo";

    /**
     * During get, the nested key specified in DefinitionMetadata[PARENTCHILDRELINFO] that indicates parent infos.
     */
    public static final String PARENTINFOS = "parentInfos";

    /**
     * During get, the nested key specified in DefinitionMetadata[PARENTCHILDRELINFO] that indicates child infos.
     */
    public static final String CHILDINFOS = "childInfos";

    private ParentChildRelMetadataConstants() {

    }

}
