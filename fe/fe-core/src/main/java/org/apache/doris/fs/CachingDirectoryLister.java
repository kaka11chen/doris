// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.fs;

import org.apache.doris.backup.Status;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.fs.remote.RemoteFile;

// import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
// import com.google.common.cache.CacheLoader;
// import com.google.common.cache.Weigher;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CachingDirectoryLister
        implements DirectoryLister
{
    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
    private final Cache<String, ValueHolder> cache;
    private final List<SchemaTablePrefix> tablePrefixes;

    public CachingDirectoryLister(OptionalLong expireAfterWriteSec, long maxSize, List<String> tables)
    {
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<String, ValueHolder>) (key, value) -> toIntExact(1000))
                // .weigher((Weigher<String, ValueHolder>) (key, value) -> toIntExact(estimatedSizeOf(key.toString()) + value.getRetainedSizeInBytes()))
                .expireAfterWrite(expireAfterWriteSec.orElseGet(() -> 60L), TimeUnit.SECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();

        // CacheFactory cacheFactory = new CacheFactory(
        //         expireAfterWriteSec,
        //         OptionalLong.empty(),
        //         maxSize,
        //         true,
        //         null);
        // this.cache = cacheFactory.buildCache();
        this.tablePrefixes = tables.stream()
                .map(CachingDirectoryLister::parseTableName)
                .collect(toImmutableList());
    }

    private static SchemaTablePrefix parseTableName(String tableName)
    {
        if (tableName.equals("*")) {
            return new SchemaTablePrefix();
        }
        String[] parts = tableName.split("\\.");
        checkArgument(parts.length == 2, "Invalid schemaTableName: %s", tableName);
        String schema = parts[0];
        String table = parts[1];
        if (table.equals("*")) {
            return new SchemaTablePrefix(schema);
        }
        return new SchemaTablePrefix(schema, table);
    }

    @Override
    public RemoteIterator<RemoteFile> listFilesRecursively(FileSystem fs, TableIf table, String location)
            throws IOException
    {
        if (!isCacheEnabledFor(SchemaTableName.schemaTableName(table.getName(), table.getDatabase().getFullName()))) {
            List<RemoteFile> result = new ArrayList<>();
            Status status = fs.listFiles(location, false, result);
            if (!status.ok()) {
                throw new IOException(status.getErrMsg());
            }
            return new RemoteFileRemoteIterator(result);
        }

        return listInternal(fs, location);
    }

    private RemoteIterator<RemoteFile> listInternal(FileSystem fs, String location)
            throws IOException
    {
        ValueHolder cachedValueHolder;
        try {
            cachedValueHolder = cache.get(location, ValueHolder::new);
        }
        catch (ExecutionException e) {
            // this can not happen because a supplier can not throw a checked exception
            throw new RuntimeException("Unexpected checked exception from cache load", e);
        }
        if (cachedValueHolder.getFiles().isPresent()) {
            return new SimpleRemoteIterator(cachedValueHolder.getFiles().get().iterator());
        }

        return cachingRemoteIterator(cachedValueHolder, createListingRemoteIterator(fs, location), location);
    }

    private static RemoteIterator<RemoteFile> createListingRemoteIterator(FileSystem fs, String location)
            throws IOException
    {
        List<RemoteFile> result = new ArrayList<>();
        Status status = fs.listFiles(location, false, result);
        if (!status.ok()) {
            throw new IOException(status.getErrMsg());
        }
        return new RemoteFileRemoteIterator(result);
    }

    // @Override
    // public void invalidate(Table table)
    // {
    //     if (isCacheEnabledFor(table.getSchemaTableName()) && isLocationPresent(table.getStorage())) {
    //         if (table.getPartitionColumns().isEmpty()) {
    //             cache.invalidate(Location.of(table.getStorage().getLocation()));
    //         }
    //         else {
    //             // a partitioned table can have multiple paths in cache
    //             cache.invalidateAll();
    //         }
    //     }
    // }
    //
    // @Override
    // public void invalidate(Partition partition)
    // {
    //     if (isCacheEnabledFor(partition.getSchemaTableName()) && isLocationPresent(partition.getStorage())) {
    //         cache.invalidate(Location.of(partition.getStorage().getLocation()));
    //     }
    // }

    private RemoteIterator<RemoteFile> cachingRemoteIterator(ValueHolder cachedValueHolder, RemoteIterator<RemoteFile> iterator, String location)
    {
        return new RemoteIterator<RemoteFile>()
        {
            private final List<RemoteFile> files = new ArrayList<>();

            @Override
            public boolean hasNext()
                    throws IOException
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    // The cachedValueHolder acts as an invalidation guard. If a cache invalidation happens while this iterator goes over
                    // the files from the specified path, the eventually outdated file listing will not be added anymore to the cache.
                    cache.asMap().replace(location, cachedValueHolder, new ValueHolder(files));
                }
                return hasNext;
            }

            @Override
            public RemoteFile next()
                    throws IOException
            {
                RemoteFile next = iterator.next();
                files.add(next);
                return next;
            }
        };
    }

    @VisibleForTesting
    boolean isCached(String location)
    {
        ValueHolder cached = cache.getIfPresent(location);
        return cached != null && cached.getFiles().isPresent();
    }

    private boolean isCacheEnabledFor(SchemaTableName schemaTableName)
    {
        return tablePrefixes.stream().anyMatch(prefix -> prefix.matches(schemaTableName));
    }

    // private static boolean isLocationPresent(Storage storage)
    // {
    //     // Some Hive table types (e.g.: views) do not have a storage location
    //     return storage.getOptionalLocation().isPresent() && !storage.getLocation().isEmpty();
    // }

    /**
     * The class enforces intentionally object identity semantics for the value holder,
     * not value-based class semantics to correctly act as an invalidation guard in the
     * cache.
     */
    private static class ValueHolder
    {
        private final Optional<List<RemoteFile>> files;

        public ValueHolder()
        {
            files = Optional.empty();
        }

        public ValueHolder(List<RemoteFile> files)
        {
            this.files = Optional.of(ImmutableList.copyOf(requireNonNull(files, "files is null")));
        }

        public Optional<List<RemoteFile>> getFiles()
        {
            return files;
        }
    }
}
