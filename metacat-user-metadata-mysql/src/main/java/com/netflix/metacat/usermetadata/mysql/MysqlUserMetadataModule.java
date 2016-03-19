package com.netflix.metacat.usermetadata.mysql;

import com.google.inject.AbstractModule;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.usermetadata.LookupService;
import com.netflix.metacat.common.usermetadata.TagService;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.DataSourceManager;

public class MysqlUserMetadataModule extends AbstractModule {
    @Override
    protected void configure() {
        requireBinding(MetacatJson.class);
        requireBinding(DataSourceManager.class);
        binder().bind(UserMetadataService.class).to(MysqlUserMetadataService.class).asEagerSingleton();
        binder().bind(LookupService.class).to(MySqlLookupService.class).asEagerSingleton();
        binder().bind(TagService.class).to(MySqlTagService.class).asEagerSingleton();
    }
}
