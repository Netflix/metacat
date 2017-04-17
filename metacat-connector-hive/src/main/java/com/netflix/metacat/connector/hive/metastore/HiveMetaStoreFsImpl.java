/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

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
 * @since 1.0.0
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
