package com.netflix.metacat.main.services;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;

import javax.annotation.Nonnull;
import java.util.List;

public interface CatalogService {
    /**
     * @return the information about the given catalog
     * @throws javax.ws.rs.NotFoundException if the catalog is not found
     */
    @Nonnull
    CatalogDto get(@Nonnull QualifiedName name);

    /**
     * @return all of the registered catalogs
     * @throws javax.ws.rs.NotFoundException if there are no registered catalogs
     */
    @Nonnull
    List<CatalogMappingDto> getCatalogNames();
}
