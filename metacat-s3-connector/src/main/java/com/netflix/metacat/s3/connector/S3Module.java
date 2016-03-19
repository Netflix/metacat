package com.netflix.metacat.s3.connector;

import com.facebook.presto.hive.NoAccessControl;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.netflix.metacat.s3.connector.dao.DatabaseDao;
import com.netflix.metacat.s3.connector.dao.FieldDao;
import com.netflix.metacat.s3.connector.dao.PartitionDao;
import com.netflix.metacat.s3.connector.dao.SourceDao;
import com.netflix.metacat.s3.connector.dao.TableDao;
import com.netflix.metacat.s3.connector.dao.impl.DatabaseDaoImpl;
import com.netflix.metacat.s3.connector.dao.impl.FieldDaoImpl;
import com.netflix.metacat.s3.connector.dao.impl.PartitionDaoImpl;
import com.netflix.metacat.s3.connector.dao.impl.SourceDaoImpl;
import com.netflix.metacat.s3.connector.dao.impl.TableDaoImpl;
import com.netflix.metacat.s3.connector.util.ConverterUtil;

/**
 * Created by amajumdar on 10/9/15.
 */
public class S3Module implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(ConnectorMetadata.class).to(S3DetailMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(S3SplitDetailManager.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorAccessControl.class).to(NoAccessControl.class).in(Scopes.SINGLETON);

        binder.bind(ConverterUtil.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseDao.class).to(DatabaseDaoImpl.class);
        binder.bind(PartitionDao.class).to(PartitionDaoImpl.class);
        binder.bind(SourceDao.class).to(SourceDaoImpl.class);
        binder.bind(TableDao.class).to(TableDaoImpl.class);
        binder.bind(FieldDao.class).to(FieldDaoImpl.class);
    }
}
