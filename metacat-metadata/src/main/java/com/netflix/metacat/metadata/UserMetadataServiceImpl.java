package com.netflix.metacat.metadata;

import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.BaseUserMetadataService;
import com.netflix.metacat.common.server.usermetadata.MetadataInterceptor;
import com.netflix.metacat.metadata.store.UserMetadataStoreService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The Hibernate-based User metadata service implementation.
 *
 * @author rveeramacheneni
 */
public class UserMetadataServiceImpl extends BaseUserMetadataService {

    private final UserMetadataStoreService userMetadataStoreService;
    private final MetacatJson metacatJson;
    private final Config config;
    private final MetadataInterceptor metadataInterceptor;

    /**
     * Ctor.
     *
     * @param userMetadataStoreService The User metadata store service.
     * @param metacatJson              The Metacat jackson JSON mapper.
     * @param config                   The config.
     * @param metadataInterceptor      The metadata interceptor.
     */
    @Autowired
    public UserMetadataServiceImpl(final UserMetadataStoreService userMetadataStoreService,
                                   final MetacatJson metacatJson,
                                   final Config config,
                                   final MetadataInterceptor metadataInterceptor) {
        this.userMetadataStoreService = userMetadataStoreService;
        this.metacatJson = metacatJson;
        this.config = config;
        this.metadataInterceptor = metadataInterceptor;
    }
}
