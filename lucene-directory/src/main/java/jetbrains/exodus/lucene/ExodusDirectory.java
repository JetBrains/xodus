/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.lucene;

import jetbrains.exodus.env.ContextualEnvironment;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.vfs.*;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class ExodusDirectory extends Directory {

    private static final int FIRST_CLUSTER_SIZE = 65536;
    private static final int MAX_CLUSTER_SIZE = 65536 * 16;

    private final ContextualEnvironment env;
    private final VirtualFileSystem vfs;
    private AtomicLong ticks = new AtomicLong(System.currentTimeMillis());

    public ExodusDirectory(@NotNull final ContextualEnvironment env) {
        this(env, StoreConfig.WITHOUT_DUPLICATES);
    }

    public ExodusDirectory(@NotNull final ContextualEnvironment env,
                           @NotNull final StoreConfig contentsStoreConfig) {
        this(env, createDefaultVfsConfig(), contentsStoreConfig);
    }

    public ExodusDirectory(@NotNull final ContextualEnvironment env,
                           @NotNull final VfsConfig vfsConfig,
                           @NotNull final StoreConfig contentsStoreConfig) {
        this.env = env;
        vfs = new VirtualFileSystem(env, vfsConfig, contentsStoreConfig);
    }

    public ContextualEnvironment getEnvironment() {
        return env;
    }

    public VirtualFileSystem getVfs() {
        return vfs;
    }

    @Override
    public String[] listAll() {
        final Transaction txn = env.getAndCheckCurrentTransaction();
        final ArrayList<String> allFiles = new ArrayList<>((int) vfs.getNumberOfFiles(txn));
        for (final File file : vfs.getFiles(txn)) {
            allFiles.add(file.getPath());
        }
        return allFiles.toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) {
        vfs.deleteFile(env.getAndCheckCurrentTransaction(), name);
    }

    @Override
    public long fileLength(String name) {
        return vfs.getFileLength(env.getAndCheckCurrentTransaction(), openExistingFile(name));
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return new ExodusIndexOutput(this, name);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        return createOutput(IndexFileNames.segmentFileName(prefix, suffix + '_' + nextTicks(), "tmp"), context);
    }

    @Override
    public void sync(Collection<String> names) {
        syncMetaData();
    }

    @Override
    public void rename(String source, String dest) {
        vfs.renameFile(env.getAndCheckCurrentTransaction(), openExistingFile(source), dest);
    }

    @Override
    public void syncMetaData() {
        ((EnvironmentImpl) env).flushAndSync();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        try {
            return new ExodusIndexInput(this, name);
        } catch (FileNotFoundException e) {
            // if index doesn't exist Lucene awaits an IOException
            throw new java.io.FileNotFoundException(name);
        }
    }

    @Override
    public Lock obtainLock(String name) {
        return NoLockFactory.INSTANCE.obtainLock(this, name);
    }

    @Override
    public void close() {
        vfs.shutdown();
    }

    long nextTicks() {
        return ticks.getAndIncrement();
    }

    @NotNull
    File openExistingFile(@NotNull final String name) {
        final File result = vfs.openFile(env.getAndCheckCurrentTransaction(), name, false);
        if (result == null) {
            throw new FileNotFoundException(name);
        }
        return result;
    }

    private static VfsConfig createDefaultVfsConfig() {
        final VfsConfig result = new VfsConfig();
        final ClusteringStrategy clusteringStrategy = new ClusteringStrategy.QuadraticClusteringStrategy(FIRST_CLUSTER_SIZE);
        clusteringStrategy.setMaxClusterSize(MAX_CLUSTER_SIZE);
        result.setClusteringStrategy(clusteringStrategy);
        return result;
    }
}
