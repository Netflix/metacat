package com.netflix.metacat.thrift;

public interface CatalogThriftServiceFactory {
    CatalogThriftService create(String catalogName, int portNumber);
}
