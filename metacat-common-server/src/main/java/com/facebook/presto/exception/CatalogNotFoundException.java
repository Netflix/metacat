package com.facebook.presto.exception;

import com.facebook.presto.spi.NotFoundException;

/**
 * Created by amajumdar on 4/30/15.
 */
public class CatalogNotFoundException extends NotFoundException{
    private final String catalogName;
    public CatalogNotFoundException(String catalogName) {
        this(catalogName, null);
    }

    public CatalogNotFoundException(String catalogName, Throwable cause) {
        super(String.format("Catalog %s not found.", catalogName), cause);
        this.catalogName = catalogName;
    }
}
