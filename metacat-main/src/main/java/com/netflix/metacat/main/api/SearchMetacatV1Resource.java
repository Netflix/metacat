package com.netflix.metacat.main.api;

import com.netflix.metacat.common.api.SearchMetacatV1;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.main.services.search.ElasticSearchUtil;

import javax.inject.Inject;
import java.util.List;

/**
 * Created by amajumdar on 12/17/15.
 */
public class SearchMetacatV1Resource implements SearchMetacatV1 {
    ElasticSearchUtil elasticSearchUtil;

    @Inject
    public SearchMetacatV1Resource(ElasticSearchUtil elasticSearchUtil) {
        this.elasticSearchUtil = elasticSearchUtil;
    }
    @Override
    public List<TableDto> searchTables(String searchString) {
        return elasticSearchUtil.simpleSearch(searchString);
    }
}
