/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

import jetbrains.exodus.Backupable;
import jetbrains.exodus.core.dataStructures.SoftLongObjectCache;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public abstract class BlobVault implements BlobHandleGenerator, Backupable {

    private static final int STRING_CONTENT_CACHE_SIZE = 0x1000;
    private static final int READ_BUFFER_SIZE = 0x4000;

    private final Object stringContentCacheLock = new Object();
    private SoftLongObjectCache<String> stringContentCache;
    protected final ByteArraySpinAllocator bufferAllocator;

    protected BlobVault() {
        stringContentCache = new SoftLongObjectCache<>(STRING_CONTENT_CACHE_SIZE);
        bufferAllocator = new ByteArraySpinAllocator(READ_BUFFER_SIZE);
    }

    @Nullable
    public abstract InputStream getContent(final long blobHandle, @NotNull final Transaction txn);

    public abstract long getSize(final long blobHandle, @NotNull final Transaction txn);

    public abstract boolean requiresTxn();

    public abstract void flushBlobs(@Nullable final LongHashMap<InputStream> blobStreams,
                                    @Nullable final LongHashMap<File> blobFiles,
                                    @Nullable final LongSet deferredBlobsToDelete,
                                    @NotNull final Transaction txn) throws Exception;

    public abstract long size();

    public abstract void close();

    public void setStringContentCacheSize(final int cacheSize) {
        synchronized (stringContentCacheLock) {
            stringContentCache = new SoftLongObjectCache<>(cacheSize);
        }
    }

    @Nullable
    public final String getStringContent(final long blobHandle, @NotNull final Transaction txn) throws IOException {
        String result;
        synchronized (stringContentCacheLock) {
            result = stringContentCache.tryKey(blobHandle);
        }
        if (result == null) {
            final InputStream content = getContent(blobHandle, txn);
            result = content == null ? null : UTFUtil.readUTF(content);
            if (result != null) {
                synchronized (stringContentCacheLock) {
                    if (stringContentCache.getObject(blobHandle) == null) {
                        stringContentCache.cacheObject(blobHandle, result);
                    }
                }
            }
        }
        return result;
    }

    public final double getStringContentCacheHitRate() {
        synchronized (stringContentCacheLock) {
            return stringContentCache.hitRate();
        }
    }

    public final ByteArrayOutputStream copyStream(@NotNull final InputStream source,
                                                  final boolean closeSource) throws IOException {
        final ByteArrayOutputStream memCopy = new LightByteArrayOutputStream();
        IOUtil.copyStreams(source, memCopy, bufferAllocator);
        if (closeSource) {
            source.close();
        }
        return memCopy;
    }

    public final ByteArraySizedInputStream cloneStream(@NotNull final InputStream source,
                                                       final boolean closeSource) throws IOException {
        final ByteArrayOutputStream memCopy = copyStream(source, closeSource);
        return new ByteArraySizedInputStream(memCopy.toByteArray(), 0, memCopy.size());
    }
}
