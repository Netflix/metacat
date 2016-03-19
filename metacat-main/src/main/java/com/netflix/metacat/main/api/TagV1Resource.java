package com.netflix.metacat.main.api;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.TagV1;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.usermetadata.TagService;
import com.netflix.metacat.common.util.MetacatContextManager;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

import static com.netflix.metacat.main.api.RequestWrapper.qualifyName;
import static com.netflix.metacat.main.api.RequestWrapper.requestWrapper;

/**
 * Created by amajumdar on 6/28/15.
 */
public class TagV1Resource implements TagV1{
    TagService tagService;
    MetacatEventBus eventBus;
    @Inject
    public TagV1Resource( MetacatEventBus eventBus, TagService tagService) {
        this.tagService = tagService;
        this.eventBus = eventBus;
    }

    @Override
    public Set<String> getTags() {
        return requestWrapper("TagV1Resource.getTags" , tagService::getTags);
    }

    @Override
    public List<QualifiedName> list(
            Set<String> includeTags,
            Set<String> excludeTags,
            String sourceName,
            String databaseName,
            String tableName) {
        return requestWrapper("TagV1Resource.list" , () -> tagService.list( includeTags, excludeTags, sourceName, databaseName, tableName));
    }

    @Override
    public List<QualifiedName> search(
            String tag,
            String sourceName,
            String databaseName,
            String tableName) {
        return requestWrapper("TagV1Resource.search" , () -> tagService.search( tag, sourceName, databaseName, tableName));
    }

    @Override
    public Set<String> setTableTags(
            String catalogName,
            String databaseName,
            String tableName,
            Set<String> tags) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper("TagV1Resource.setTableTags" , () -> {
            Set<String> result = tagService.setTableTags( name, tags, true);

            eventBus.post(new MetacatUpdateTablePostEvent(name, metacatContext));
            return result;
        });
    }

    @Override
    public void removeTableTags(
            String catalogName,
            String databaseName,
            String tableName,
            Boolean deleteAll,
            Set<String> tags) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        requestWrapper("TagV1Resource.removeTableTags" , () -> {
            tagService.removeTableTags( name, deleteAll, tags, true);

            eventBus.post(new MetacatUpdateTablePostEvent(name, metacatContext));
            return null;
        });
    }
}
