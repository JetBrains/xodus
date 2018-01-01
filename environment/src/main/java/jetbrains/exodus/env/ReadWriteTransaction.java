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
package jetbrains.exodus.env;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.decorators.HashMapDecorator;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.log.ExpiredLoggableInfo;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.TreeMetaInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static jetbrains.exodus.env.EnvironmentStatistics.Type.TRANSACTIONS;

public class ReadWriteTransaction extends TransactionBase {

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
    private int replayCount;

    ReadWriteTransaction(@NotNull final EnvironmentImpl env,
                         @Nullable final Runnable beginHook,
                         final boolean isExclusive,
                         final boolean cloneMeta) {
        super(env, isExclusive);
        mutableTrees = new TreeMap<>();
        removedStores = new LongHashMap<>();
        createdStores = new HashMapDecorator<>();
        this.beginHook = new Runnable() {
            @Override
            public void run() {
                final MetaTree currentMetaTree = env.getMetaTree();
                setMetaTree(cloneMeta ? currentMetaTree.getClone() : currentMetaTree);
                env.registerTransaction(ReadWriteTransaction.this);
                if (beginHook != null) {
                    beginHook.run();
                }
            }
        };
        replayCount = 0;
        setExclusive(isExclusive() | env.shouldTransactionBeExclusive(this));
        env.holdNewestSnapshotBy(this);
        env.getStatistics().getStatisticsItem(TRANSACTIONS).incTotal();
    }

    ReadWriteTransaction(@NotNull final TransactionBase origin, @Nullable final Runnable beginHook) {
        super(origin.getEnvironment(), false);
        mutableTrees = new TreeMap<>();
        removedStores = new LongHashMap<>();
        createdStores = new HashMapDecorator<>();
        final EnvironmentImpl env = getEnvironment();
        this.beginHook = getWrappedBeginHook(beginHook);
        replayCount = 0;
        setMetaTree(origin.getMetaTree());
        setExclusive(env.shouldTransactionBeExclusive(this));
        env.acquireTransaction(this);
        env.registerTransaction(this);
        env.getStatistics().getStatisticsItem(TRANSACTIONS).incTotal();
    }

    public boolean isIdempotent() {
        return mutableTrees.isEmpty() && removedStores.isEmpty() && createdStores.isEmpty();
    }

    @Override
    public void abort() {
        checkIsFinished();
        clearImmutableTrees();
        doRevert();
        getEnvironment().finishTransaction(this);
    }

    @Override
    public boolean commit() {
        checkIsFinished();
        return getEnvironment().commitTransaction(this, false);
    }

    @Override
    public boolean flush() {
        checkIsFinished();
        final EnvironmentImpl env = getEnvironment();
        final boolean result = env.flushTransaction(this, false);
        if (result) {
            // if the transaction was upgraded to exclusive during re-playing
            // then it should be downgraded back after successful flush().
            if (!wasCreatedExclusive() && isExclusive() && env.getEnvironmentConfig().getEnvTxnDowngradeAfterFlush()) {
                env.downgradeTransaction(this);
                setExclusive(false);
            }
            setStarted(System.currentTimeMillis());
        } else {
            incReplayCount();
        }
        return result;
    }

    @Override
    public void revert() {
        checkIsFinished();
        if (isReadonly()) {
            throw new ExodusException("Attempt ot revert read-only transaction");
        }
        final long oldRoot = getMetaTree().root;
        final boolean wasExclusive = isExclusive();
        final EnvironmentImpl env = getEnvironment();
        if (isIdempotent()) {
            env.holdNewestSnapshotBy(this, false);
        } else {
            doRevert();
            if (wasExclusive || !env.shouldTransactionBeExclusive(this)) {
                env.holdNewestSnapshotBy(this, false);
            } else {
                env.releaseTransaction(this);
                setExclusive(true);
                env.holdNewestSnapshotBy(this);
            }
        }
        if (!env.isRegistered(this)) {
            throw new ExodusException("Transaction should remain registered after revert");
        }
        if (!checkVersion(oldRoot)) {
            clearImmutableTrees();
            env.runTransactionSafeTasks();
        }
        setStarted(System.currentTimeMillis());
    }

    @Override
    public void setCommitHook(@Nullable final Runnable hook) {
        commitHook = hook;
    }

    @Override
    public boolean isReadonly() {
        return false;
    }

    public boolean forceFlush() {
        checkIsFinished();
        return getEnvironment().flushTransaction(this, true);
    }

    @NotNull
    public StoreImpl openStoreByStructureId(final int structureId) {
        checkIsFinished();
        final EnvironmentImpl env = getEnvironment();
        final String storeName = getMetaTree().getStoreNameByStructureId(structureId, env);
        return storeName == null ?
            new TemporaryEmptyStore(env) :
            env.openStoreImpl(storeName, StoreConfig.USE_EXISTING, this, getTreeMetaInfo(storeName));
    }

    @NotNull
    @Override
    public ITree getTree(@NotNull final StoreImpl store) {
        checkIsFinished();
        final ITreeMutable result = mutableTrees.get(store.getStructureId());
        if (result == null) {
            return super.getTree(store);
        }
        return result;
    }

    @Nullable
    @Override
    TreeMetaInfo getTreeMetaInfo(@NotNull final String name) {
        checkIsFinished();
        final TreeMetaInfo result = createdStores.get(name);
        return result == null ? super.getTreeMetaInfo(name) : result;
    }

    void storeRemoved(@NotNull final StoreImpl store) {
        checkIsFinished();
        super.storeRemoved(store);
        final int structureId = store.getStructureId();
        final ITree tree = store.openImmutableTree(getMetaTree());
        removedStores.put(structureId, new Pair<>(store.getName(), tree));
        mutableTrees.remove(structureId);
    }

    void storeCreated(@NotNull final StoreImpl store) {
        getMutableTree(store);
        createdStores.put(store.getName(), store.getMetaInfo());
    }

    int getReplayCount() {
        return replayCount;
    }

    void incReplayCount() {
        ++replayCount;
    }

    boolean isStoreNew(@NotNull final String name) {
        return createdStores.containsKey(name);
    }


    Iterable<ExpiredLoggableInfo>[] doCommit(@NotNull final MetaTree[] out) {
        final Set<Map.Entry<Integer, ITreeMutable>> entries = mutableTrees.entrySet();
        final Set<Map.Entry<Long, Pair<String, ITree>>> removedEntries = removedStores.entrySet();
        final int size = entries.size() + removedEntries.size();
        //noinspection unchecked
        final Iterable<ExpiredLoggableInfo>[] expiredLoggables = new Iterable[size + 1];
        int i = 0;
        final ITreeMutable metaTreeMutable = getMetaTree().tree.getMutableCopy();
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
        final Collection<ExpiredLoggableInfo> last;
        for (final Map.Entry<Integer, ITreeMutable> entry : entries) {
            final ITreeMutable treeMutable = entry.getValue();
            expiredLoggables[i++] = treeMutable.getExpiredLoggables();
            MetaTree.saveTree(metaTreeMutable, treeMutable);
        }
        clearImmutableTrees();
        mutableTrees.clear();
        expiredLoggables[i] = last = metaTreeMutable.getExpiredLoggables();
        out[0] = MetaTree.saveMetaTree(metaTreeMutable, getEnvironment(), last);
        return expiredLoggables;
    }

    void executeCommitHook() {
        if (commitHook != null) {
            commitHook.run();
        }
    }

    @NotNull
    ITreeMutable getMutableTree(@NotNull final StoreImpl store) {
        checkIsFinished();
        final Thread creatingThread = getCreatingThread();
        if (!creatingThread.equals(Thread.currentThread())) {
            throw new ExodusException("Can't create mutable tree in a thread different from the one which transaction was created in");
        }
        final int structureId = store.getStructureId();
        ITreeMutable result = mutableTrees.get(structureId);
        if (result == null) {
            result = getTree(store).getMutableCopy();
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

    @NotNull
    @Override
    List<String> getAllStoreNames() {
        List<String> result = super.getAllStoreNames();
        if (createdStores.isEmpty()) return result;
        if (result.isEmpty()) {
            result = new ArrayList<>();
        }
        result.addAll(createdStores.keySet());
        Collections.sort(result);
        return result;
    }

    @Nullable
    @Override
    Runnable getBeginHook() {
        return beginHook;
    }

    private void doRevert() {
        mutableTrees.clear();
        removedStores.clear();
        createdStores.clear();
    }
}