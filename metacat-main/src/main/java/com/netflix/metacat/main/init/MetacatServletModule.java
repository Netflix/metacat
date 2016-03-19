package com.netflix.metacat.main.init;

import com.google.inject.servlet.ServletModule;
import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.MetadataV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.api.SearchMetacatV1;
import com.netflix.metacat.common.api.TagV1;
import com.netflix.metacat.common.server.CommonModule;
import com.netflix.metacat.converters.ConvertersModule;
import com.netflix.metacat.main.api.MetacatV1Resource;
import com.netflix.metacat.main.api.MetadataV1Resource;
import com.netflix.metacat.main.api.PartitionV1Resource;
import com.netflix.metacat.main.api.SearchMetacatV1Resource;
import com.netflix.metacat.main.api.TagV1Resource;
import com.netflix.metacat.main.manager.ManagerModule;
import com.netflix.metacat.main.services.ServicesModule;
import com.netflix.metacat.thrift.ThriftModule;

public class MetacatServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
        install(new CommonModule());
        install(new ConvertersModule());
        install(new ThriftModule());
        install(new ManagerModule());
        install(new ServicesModule());

        binder().bind(MetacatV1.class).to(MetacatV1Resource.class).asEagerSingleton();
        binder().bind(PartitionV1.class).to(PartitionV1Resource.class).asEagerSingleton();
        binder().bind(MetadataV1.class).to(MetadataV1Resource.class).asEagerSingleton();
        binder().bind(SearchMetacatV1.class).to(SearchMetacatV1Resource.class).asEagerSingleton();
        binder().bind(TagV1.class).to(TagV1Resource.class).asEagerSingleton();
        binder().bind(MetacatThriftService.class).asEagerSingleton();
    }
}
