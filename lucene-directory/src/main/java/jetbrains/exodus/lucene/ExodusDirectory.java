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
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.vfs.*;
import org.apache.lucene.store.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;

public class ExodusDirectory extends Directory {

    private static final int FIRST_CLUSTER_SIZE = 65536;
    private static final int MAX_CLUSTER_SIZE = 65536 * 16;

    private final ContextualEnvironment env;
    private final VirtualFileSystem vfs;

    public ExodusDirectory(@NotNull final ContextualEnvironment env) throws IOException {
        this(env, new SingleInstanceLockFactory());
    }

    public ExodusDirectory(@NotNull final ContextualEnvironment env,
                           @NotNull final LockFactory lockFactory) throws IOException {
        this(env, StoreConfig.WITHOUT_DUPLICATES, lockFactory);
    }

    public ExodusDirectory(@NotNull final ContextualEnvironment env,
                           @NotNull final StoreConfig contentsStoreConfig,
                           @NotNull final LockFactory lockFactory) throws IOException {
        this(env, createDefaultVfsConfig(), contentsStoreConfig, lockFactory);
    }

    public ExodusDirectory(@NotNull final ContextualEnvironment env,
                           @NotNull final VfsConfig vfsConfig,
                           @NotNull final StoreConfig contentsStoreConfig,
                           @NotNull final LockFactory lockFactory) throws IOException {
        this.env = env;
        vfs = new VirtualFileSystem(env, vfsConfig, contentsStoreConfig);
        setLockFactory(lockFactory);
    }

    public ContextualEnvironment getEnvironment() {
        return env;
    }

    public VirtualFileSystem getVfs() {
        return vfs;
    }

    @Override
    public String[] listAll() throws IOException {
        final Transaction txn = env.getAndCheckCurrentTransaction();
        final ArrayList<String> allFiles = new ArrayList<>((int) vfs.getNumberOfFiles(txn));
        for (final File file : vfs.getFiles(txn)) {
            allFiles.add(file.getPath());
        }
        return allFiles.toArray(new String[allFiles.size()]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return openExistingFile(name, false) != null;
    }

    @Override
    public long fileModified(String name) throws IOException {
        final File file = openExistingFile(name, false);
        return file == null ? 0 : file.getLastModified();
    }

    @Override
    public void touchFile(String name) throws IOException {
        vfs.touchFile(env.getAndCheckCurrentTransaction(), openExistingFile(name, true));
    }

    @Override
    public void deleteFile(String name) throws IOException {
        vfs.deleteFile(env.getAndCheckCurrentTransaction(), name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return vfs.getFileLength(env.getAndCheckCurrentTransaction(), openExistingFile(name, true));
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        return new ExodusIndexOutput(this, name);
    }

    @Override
    public IndexInput openInput(String name) throws IOException {
        try {
            return new ExodusIndexInput(this, name);
        } catch (FileNotFoundException e) {
            // if index doesn't exist Lucene awaits an IOException
            throw new java.io.FileNotFoundException(name);
        }
    }

    @Override
    public void close() throws IOException {
        vfs.shutdown();
    }

    File openExistingFile(@NotNull final String name, final boolean throwFileNotFound) {
        final File result = vfs.openFile(env.getAndCheckCurrentTransaction(), name, false);
        if (throwFileNotFound && result == null) {
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
