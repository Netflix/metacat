package com.netflix.metacat.connector.hive.metastore;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreFS;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * HiveMetaStoreFsImpl.
 *
 * @author zhenl
 */
@Slf4j
public class HiveMetaStoreFsImpl implements MetaStoreFS {
    @Override
    public boolean deleteDir(final FileSystem fileSystem, final Path path,
                             final boolean b, final boolean b2, final Configuration entries)
            throws MetaException {
        log.info("No-op call for deleting '{}'", path);
        return true;
    }
}
