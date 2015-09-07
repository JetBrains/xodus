/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.env;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.decorators.HashMapDecorator;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.TreeMetaInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class TransactionImpl implements Transaction {

    @NotNull
    private final EnvironmentImpl env;
    @Nullable
    private final Thread creatingThread;
    @NotNull
    private MetaTree metaTree;
    @NotNull
    private final IntHashMap<ITree> immutableTrees;
    @NotNull
    private final Map<Integer, ITreeMutable> mutableTrees;
    @NotNull
    private final LongHashMap<Pair<String, ITree>> removedStores;
    @NotNull
    private final Map<String, TreeMetaInfo> createdStores;
    @Nullable
    private final Runnable beginHook;
    @Nullable
    private Runnable commitHook;
    @Nullable
    private final Throwable trace;
    private long started;       // started is the ticks when the txn held its current snapshot
    private final long created; // created is the ticks when the txn was actually created (constructed)
    private int replayCount;
    private boolean isExclusive;

    TransactionImpl(@NotNull final EnvironmentImpl env,
                    @Nullable final Thread creatingThread,
                    @Nullable final Runnable beginHook,
                    final boolean isExclusive,
                    final boolean cloneMeta) {
        this.env = env;
        this.creatingThread = creatingThread;
        immutableTrees = new IntHashMap<>();
        mutableTrees = new TreeMap<>();
        removedStores = new LongHashMap<>();
        createdStores = new HashMapDecorator<>();
        this.beginHook = new Runnable() {
            @Override
            public void run() {
                final MetaTree currentMetaTree = env.getMetaTree();
                metaTree = cloneMeta ? currentMetaTree.getClone() : currentMetaTree;
                env.registerTransaction(TransactionImpl.this);
                if (beginHook != null) {
                    beginHook.run();
                }
            }
        };
        trace = env.transactionTimeout() > 0 ? new Throwable() : null;
        invalidateStarted();
        created = started;
        replayCount = 0;
        this.isExclusive = isExclusive;
        holdNewestSnapshot();
    }

    /**
     * Constructor for creating new snapshot transaction.
     */
    protected TransactionImpl(@NotNull final TransactionImpl origin) {
        env = origin.env;
        metaTree = origin.metaTree;
        commitHook = origin.commitHook;
        beginHook = origin.beginHook;
        creatingThread = origin.creatingThread;
        immutableTrees = new IntHashMap<>();
        mutableTrees = new TreeMap<>();
        removedStores = new LongHashMap<>();
        createdStores = new HashMapDecorator<>();
        trace = env.transactionTimeout() > 0 ? new Throwable() : null;
        invalidateStarted();
        created = started;
        replayCount = 0;
        env.acquireTransaction(isExclusive(), isReadonly());
        env.registerTransaction(this);
    }

    public boolean isIdempotent() {
        return mutableTrees.isEmpty() && removedStores.isEmpty() && createdStores.isEmpty();
    }

    @Override
    public void abort() {
        doRevert();
        env.finishTransaction(this);
    }

    @Override
    public boolean commit() {
        return env.commitTransaction(this, false);
    }

    @Override
    public boolean flush() {
        final boolean result = env.flushTransaction(this, false);
        if (result) {
            invalidateStarted();
        } else {
            incReplayCount();
        }
        return result;
    }

    @Override
    public void revert() {
        if (isReadonly()) {
            throw new ExodusException("Attempt ot revert read-only transaction");
        }
        doRevert();
        final boolean wasExclusive = isExclusive;
        env.releaseTransaction(wasExclusive, false);
        isExclusive |= env.shouldTransactionBeExclusive(this);
        final long oldRoot = metaTree.root;
        holdNewestSnapshot();
        if (!env.isRegistered(this)) {
            throw new ExodusException("Transaction should remain registered after revert");
        }
        if (!checkVersion(oldRoot)) {
            // GUARD: if txn is exclusive then database version could not be changed
            if (wasExclusive) {
                throw new ExodusException("Meta tree modified during exclusive transaction");
            }
            env.runTransactionSafeTasks();
        }
        invalidateStarted();
    }

    @Override
    public Transaction getSnapshot() {
        return new ReadonlyTransaction(this);
    }

    @Override
    @NotNull
    public EnvironmentImpl getEnvironment() {
        return env;
    }

    @Override
    public void setCommitHook(@Nullable final Runnable hook) {
        commitHook = hook;
    }

    @Override
    public long getStartTime() {
        return started;
    }

    @Override
    public long getHighAddress() {
        return metaTree.highAddress;
    }

    @Override
    public boolean isReadonly() {
        return false;
    }

    @Override
    public boolean isExclusive() {
        return isExclusive;
    }

    public boolean forceFlush() {
        return env.flushTransaction(this, true);
    }

    @NotNull
    public StoreImpl openStoreByStructureId(final int structureId) {
        final String storeName = metaTree.getStoreNameByStructureId(structureId, env);
        return storeName == null ?
                new TemporaryEmptyStore(env) :
                env.openStoreImpl(storeName, StoreConfig.USE_EXISTING, this, env.getCurrentMetaInfo(storeName, this));
    }

    @NotNull
    public ITree getTree(@NotNull final StoreImpl store) {
        final ITreeMutable result = mutableTrees.get(store.getStructureId());
        if (result == null) {
            return getImmutableTree(store);
        }
        return result;
    }

    void storeRemoved(@NotNull final StoreImpl store) {
        final int structureId = store.getStructureId();
        final ITree tree = store.openImmutableTree(metaTree);
        removedStores.put(structureId, new Pair<>(store.getName(), tree));
        immutableTrees.remove(structureId);
        mutableTrees.remove(structureId);
    }

    void storeCreated(@NotNull final StoreImpl store) {
        getMutableTree(store);
        createdStores.put(store.getName(), store.getMetaInfo());
    }

    @Nullable
    Throwable getTrace() {
        return trace;
    }

    long getCreated() {
        return created;
    }

    int getReplayCount() {
        return replayCount;
    }

    void incReplayCount() {
        ++replayCount;
    }

    /**
     * Returns tree meta info by name of a newly created (in this transaction) store.
     */
    @Nullable
    TreeMetaInfo getNewStoreMetaInfo(@NotNull final String name) {
        return createdStores.get(name);
    }

    boolean isStoreNew(@NotNull final String name) {
        return createdStores.containsKey(name);
    }

    boolean checkVersion(final long root) {
        return metaTree.root == root;
    }

    Iterable<Loggable>[] doCommit(@NotNull final MetaTree[] out) {
        final Set<Map.Entry<Integer, ITreeMutable>> entries = mutableTrees.entrySet();
        final Set<Map.Entry<Long, Pair<String, ITree>>> removedEntries = removedStores.entrySet();
        final int size = entries.size() + removedEntries.size();
        //noinspection unchecked
        final Iterable<Loggable>[] expiredLoggables = new Iterable[size + 1];
        int i = 0;
        final ITreeMutable metaTreeMutable = metaTree.tree.getMutableCopy();
        for (final Map.Entry<Long, Pair<String, ITree>> entry : removedEntries) {
            final Pair<String, ITree> value = entry.getValue();
            MetaTree.removeStore(metaTreeMutable, value.getFirst(), entry.getKey());
            expiredLoggables[i++] = TreeMetaInfo.getTreeLoggables(value.getSecond());
        }
        removedStores.clear();
        for (final Map.Entry<String, TreeMetaInfo> entry : createdStores.entrySet()) {
            MetaTree.addStore(metaTreeMutable, entry.getKey(), entry.getValue());
        }
        createdStores.clear();
        final Collection<Loggable> last;
        for (final Map.Entry<Integer, ITreeMutable> entry : entries) {
            final ITreeMutable treeMutable = entry.getValue();
            expiredLoggables[i++] = treeMutable.getExpiredLoggables();
            MetaTree.saveTree(metaTreeMutable, treeMutable);
        }
        immutableTrees.clear();
        mutableTrees.clear();
        expiredLoggables[i] = last = metaTreeMutable.getExpiredLoggables();
        out[0] = MetaTree.saveMetaTree(metaTreeMutable, env, last);
        return expiredLoggables;
    }

    void setMetaTree(@NotNull final MetaTree metaTree) {
        this.metaTree = metaTree;
    }

    void executeCommitHook() {
        if (commitHook != null) {
            commitHook.run();
        }
    }

    @NotNull
    ITreeMutable getMutableTree(@NotNull final StoreImpl store) {
        if (creatingThread != null && !creatingThread.equals(Thread.currentThread())) {
            throw new ExodusException("Can't create mutable tree in a thread different from the one which transaction was created in");
        }
        final int structureId = store.getStructureId();
        ITreeMutable result = mutableTrees.get(structureId);
        if (result == null) {
            result = getImmutableTree(store).getMutableCopy();
            mutableTrees.put(structureId, result);
        }
        return result;
    }

    /**
     * @param store opened store.
     * @return whether a mutable tree is created for specified store.
     */
    boolean hasTreeMutable(@NotNull final StoreImpl store) {
        return mutableTrees.containsKey(store.getStructureId());
    }

    void removeTreeMutable(@NotNull final StoreImpl store) {
        mutableTrees.remove(store.getStructureId());
    }

    @Nullable
    Thread getCreatingThread() {
        return creatingThread;
    }

    @NotNull
    MetaTree getMetaTree() {
        return metaTree;
    }

    long getRoot() {
        return metaTree.root;
    }

    List<String> getAllStoreNames() {
        // TODO: optimize
        List<String> result = metaTree.getAllStoreNames();
        if (createdStores.isEmpty()) return result;
        if (result.isEmpty()) {
            result = new ArrayList<>();
        }
        result.addAll(createdStores.keySet());
        Collections.sort(result);
        return result;
    }

    @Nullable
    Runnable getBeginHook() {
        return beginHook;
    }

    private void invalidateStarted() {
        started = System.currentTimeMillis();
    }

    private void holdNewestSnapshot() {
        env.holdNewestSnapshotBy(this);
    }

    @NotNull
    private ITree getImmutableTree(@NotNull final StoreImpl store) {
        final int structureId = store.getStructureId();
        ITree result = immutableTrees.get(structureId);
        if (result == null) {
            result = store.openImmutableTree(metaTree);
            immutableTrees.put(structureId, result);
        }
        return result;
    }

    private void doRevert() {
        immutableTrees.clear();
        mutableTrees.clear();
        removedStores.clear();
        createdStores.clear();
    }
}