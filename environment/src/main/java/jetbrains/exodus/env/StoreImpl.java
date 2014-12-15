/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.IExpirationChecker;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.TreeMetaInfo;
import jetbrains.exodus.tree.btree.BTree;
import jetbrains.exodus.tree.btree.BTreeBalancePolicy;
import jetbrains.exodus.tree.btree.BTreeEmpty;
import jetbrains.exodus.tree.patricia.PatriciaTree;
import jetbrains.exodus.tree.patricia.PatriciaTreeEmpty;
import jetbrains.exodus.tree.patricia.PatriciaTreeWithDuplicates;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

@SuppressWarnings({"ClassNameSameAsAncestorName"})
public class StoreImpl implements Store {

    @NotNull
    private static final ArrayByteIterable NULL_CACHED_VALUE = new ArrayByteIterable(ByteIterable.EMPTY);

    @NotNull
    private final EnvironmentImpl environment;
    @NotNull
    private final String name;
    @NotNull
    private final TreeMetaInfo metaInfo;

    StoreImpl(@NotNull final EnvironmentImpl env, @NotNull final String name, @NotNull final TreeMetaInfo metaInfo) {
        this.environment = env;
        this.name = name;
        this.metaInfo = metaInfo;
    }

    @NotNull
    @Override
    public EnvironmentImpl getEnvironment() {
        return environment;
    }

    @Override
    @Nullable
    public ByteIterable get(@NotNull final Transaction txn, @NotNull final ByteIterable key) {
        final ITree tree = ((TransactionImpl) txn).getTree(this);
        final long treeRootAddress = tree.getRootAddress();
        final StoreGetCache storeGetCache;
        // if neither tree is empty nor mutable and StoreGetCache is on
        if (treeRootAddress != Loggable.NULL_ADDRESS && (storeGetCache = environment.getStoreGetCache()) != null) {
            ByteIterable result = storeGetCache.tryKey(treeRootAddress, key);
            if (result != null) {
                return result == NULL_CACHED_VALUE ? null : result;
            }
            result = tree.get(key);
            storeGetCache.cacheObject(treeRootAddress, key, result == null ? NULL_CACHED_VALUE : new ArrayByteIterable(result));
            return result;
        }
        return tree.get(key);
    }

    @Override
    public boolean exists(@NotNull final Transaction txn,
                          @NotNull final ByteIterable key,
                          @NotNull final ByteIterable data) {
        return ((TransactionImpl) txn).getTree(this).hasPair(key, data);
    }

    @Override
    public boolean put(@NotNull final Transaction txn,
                       @NotNull final ByteIterable key,
                       @NotNull final ByteIterable value) {
        return ((TransactionImpl) txn).getMutableTree(this).put(key, value);
    }

    @Override
    public void putRight(@NotNull final Transaction txn,
                         @NotNull final ByteIterable key,
                         @NotNull final ByteIterable value) {
        ((TransactionImpl) txn).getMutableTree(this).putRight(key, value);
    }

    @Override
    public boolean add(@NotNull final Transaction txn,
                       @NotNull final ByteIterable key,
                       @NotNull final ByteIterable value) {
        return ((TransactionImpl) txn).getMutableTree(this).add(key, value);
    }

    @Override
    public long count(@NotNull Transaction txn) {
        return ((TransactionImpl) txn).getTree(this).getSize();
    }

    @Override
    public Cursor openCursor(@NotNull final Transaction txn) {
        return new CursorImpl(this, (TransactionImpl) txn);
    }

    @Override
    public boolean delete(@NotNull final Transaction txn,
                          @NotNull final ByteIterable key) {
        return ((TransactionImpl) txn).getMutableTree(this).delete(key);
    }

    @Override
    @NotNull
    public String getName() {
        return name;
    }

    @Override
    public boolean isNew(@NotNull Transaction txn) {
        return ((TransactionImpl) txn).isStoreNew(name);
    }

    @Override
    public void persistCreation(@NotNull Transaction txn) {
        ((TransactionImpl) txn).storeCreated(this);
    }

    @Override
    public void close() {
    }

    @Override
    @NotNull
    public StoreConfig getConfig() {
        return TreeMetaInfo.toConfig(metaInfo);
    }

    @NotNull
    TreeMetaInfo getMetaInfo() {
        return metaInfo;
    }

    public void reclaim(@NotNull final Transaction txn,
                        @NotNull final RandomAccessLoggable loggable,
                        @NotNull final Iterator<RandomAccessLoggable> loggables,
                        @NotNull final IExpirationChecker expirationChecker) {
        final TransactionImpl jt = (TransactionImpl) txn;
        final boolean wasTreeCreated = jt.hasTreeMutable(this);
        if (!jt.getMutableTree(this).reclaim(loggable, loggables, expirationChecker) && !wasTreeCreated) {
            jt.removeTreeMutable(this);
        }
    }

    public ITree getUpToDateTree() {
        return openImmutableTree(environment.getMetaTree(null));
    }

    public ITree openImmutableTree(@NotNull final MetaTree metaTree) {
        final long structureId = getStructureId();
        final long upToDateRootAddress = metaTree.getRootAddress(structureId);
        final boolean hasDuplicates = metaInfo.hasDuplicates();
        final boolean treeIsEmpty = upToDateRootAddress == Loggable.NULL_ADDRESS;
        final Log log = environment.getLog();
        final ITree result;
        if (!metaInfo.isKeyPrefixing()) {
            final BTreeBalancePolicy balancePolicy = environment.getBTreeBalancePolicy();
            result = treeIsEmpty ?
                    new BTreeEmpty(log, balancePolicy, hasDuplicates, structureId) :
                    new BTree(log, upToDateRootAddress, balancePolicy, hasDuplicates, structureId);
        } else {
            if (treeIsEmpty) {
                result = new PatriciaTreeEmpty(hasDuplicates, log, structureId);
            } else {
                result = hasDuplicates ?
                        new PatriciaTreeWithDuplicates(log, upToDateRootAddress, structureId) :
                        new PatriciaTree(log, upToDateRootAddress, structureId);
            }
        }
        return result;
    }

    long getStructureId() {
        return metaInfo.getStructureId();
    }
}