/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.*;
import jetbrains.exodus.tree.btree.BTree;
import jetbrains.exodus.tree.btree.BTreeEmpty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class MetaTreeImpl implements MetaTree {

    private static final int EMPTY_LOG_BOUND = 5;

    final ITree tree;
    final long root;

    MetaTreeImpl(final ITree tree, long root) {
        this.tree = tree;
        this.root = root;
    }

    static Pair<MetaTreeImpl, Integer> create(@NotNull final EnvironmentImpl env,
                                              @NotNull ExpiredLoggableCollection expired) {
        final Log log = env.getLog();
        final long highAddress = log.getHighAddress();
        if (highAddress > EMPTY_LOG_BOUND) {
            Loggable rootLoggable;
            final long rootAddress = log.getStartUpDbRoot();

            if (rootAddress >= 0) {
                rootLoggable = log.read(rootAddress);
            } else {
                rootLoggable = null;
            }

            if (rootLoggable != null) {
                final long root = rootLoggable.getAddress();
                DatabaseRoot dbRoot = null;
                try {
                    dbRoot = new DatabaseRoot(rootLoggable);
                } catch (ExodusException e) {
                    EnvironmentImpl.loggerError("Failed to load database root at " + rootLoggable.getAddress(), e);
                }
                if (dbRoot != null && dbRoot.isValid()) {
                    try {
                        final BTree metaTree = env.loadMetaTree(dbRoot.getRootAddress());
                        if (metaTree != null) {
                            return new Pair<>(new MetaTreeImpl(metaTree, root),
                                    dbRoot.getLastStructureId());
                        }
                    } catch (ExodusException e) {
                        EnvironmentImpl.loggerError("Failed to recover to valid root" +
                                LogUtil.getWrongAddressErrorMessage(dbRoot.getAddress(),
                                        env.getEnvironmentConfig().getLogFileSize() << 10), e);
                    }
                }
            }
            try {
                DataCorruptionException.raise("No valid root has found in the database", log, rootAddress);
            } finally {
                log.close();
            }
        }
        // no roots found: the database is empty
        EnvironmentImpl.loggerDebug("No roots found: the database is empty");
        final ITree resultTree = getEmptyMetaTree(env);
        final long root;
        log.beginWrite();
        try {
            final long rootAddress = resultTree.getMutableCopy().save();
            root = log.write(DatabaseRoot.DATABASE_ROOT_TYPE, Loggable.NO_STRUCTURE_ID,
                    DatabaseRoot.asByteIterable(rootAddress, EnvironmentImpl.META_TREE_ID), expired);
            log.flush();
            log.endWrite();
        } catch (Throwable t) {
            throw new ExodusException("Can't init meta tree in log", t);
        }
        return new Pair<>(new MetaTreeImpl(resultTree, root),
                EnvironmentImpl.META_TREE_ID);
    }

    static MetaTreeImpl create(@NotNull final EnvironmentImpl env,
                               @NotNull final MetaTreePrototype prototype) {
        return new MetaTreeImpl(
                env.loadMetaTree(prototype.treeAddress()),
                prototype.rootAddress()
        );
    }

    @Override
    public long treeAddress() {
        return tree.getRootAddress();
    }

    @Override
    public long rootAddress() {
        return root;
    }

    LongIterator addressIterator() {
        return tree.addressIterator();
    }

    @Nullable
    TreeMetaInfo getMetaInfo(@NotNull final String storeName, @NotNull final EnvironmentImpl env) {
        final ByteIterable value = tree.get(StringBinding.stringToEntry(storeName));
        if (value == null) {
            return null;
        }
        return TreeMetaInfo.load(env, value);
    }

    long getRootAddress(final int structureId) {
        final ByteIterable value = tree.get(LongBinding.longToCompressedEntry(structureId));
        return value == null ? Loggable.NULL_ADDRESS : CompressedUnsignedLongByteIterable.getLong(value);
    }

    static void removeStore(@NotNull final ITreeMutable out, @NotNull final String storeName, final long id) {
        out.delete(StringBinding.stringToEntry(storeName));
        out.delete(LongBinding.longToCompressedEntry(id));
    }

    static void addStore(@NotNull final ITreeMutable out, @NotNull final String storeName, @NotNull final TreeMetaInfo metaInfo) {
        out.put(StringBinding.stringToEntry(storeName), metaInfo.toByteIterable());
    }

    static void saveTree(@NotNull final ITreeMutable out,
                         @NotNull final ITreeMutable treeMutable) {
        final long treeRootAddress = treeMutable.save();
        final int structureId = treeMutable.getStructureId();
        out.put(LongBinding.longToCompressedEntry(structureId),
                CompressedUnsignedLongByteIterable.getIterable(treeRootAddress));
    }

    /**
     * Saves meta tree, writes database root and flushes the log.
     *
     * @param metaTree mutable meta tree
     * @param env      enclosing environment
     * @param expired  expired loggables (database root to be added)
     * @return database root loggable which is read again from the log.
     */
    @NotNull
    static MetaTreeImpl.Proto saveMetaTree(@NotNull final ITreeMutable metaTree,
                                           @NotNull final EnvironmentImpl env,
                                           @NotNull final ExpiredLoggableCollection expired) {
        final long newMetaTreeAddress = metaTree.save();
        final Log log = env.getLog();
        final int lastStructureId = env.getLastStructureId();
        final long dbRootAddress = log.write(DatabaseRoot.DATABASE_ROOT_TYPE, Loggable.NO_STRUCTURE_ID,
                DatabaseRoot.asByteIterable(newMetaTreeAddress, lastStructureId), expired);
        expired.add(dbRootAddress, (int) (log.getWrittenHighAddress() - dbRootAddress));
        return new MetaTreeImpl.Proto(newMetaTreeAddress, dbRootAddress);
    }

    long getAllStoreCount() {
        long size = tree.getSize();
        if (size % 2L != 0) {
            EnvironmentImpl.loggerError("MetaTree size is not even");
        }
        return size / 2;
    }

    @NotNull
    List<String> getAllStoreNames() {
        final ITree tree = this.tree;
        if (tree.getSize() == 0) {
            return Collections.emptyList();
        }
        final List<String> result = new ArrayList<>();
        try (ITreeCursor cursor = tree.openCursor()) {
            //noinspection MethodCallInLoopCondition
            while (cursor.getNext()) {
                @SuppressWarnings("ObjectAllocationInLoop") final ArrayByteIterable key = new ArrayByteIterable(cursor.getKey());
                if (isStringKey(key)) {
                    final String storeName = StringBinding.entryToString(key);
                    if (!EnvironmentImpl.isUtilizationProfile(storeName)) {
                        result.add(storeName);
                    }
                }
            }
        }
        return result;
    }

    @Nullable
    String getStoreNameByStructureId(final int structureId, @NotNull final EnvironmentImpl env) {
        try (ITreeCursor cursor = tree.openCursor()) {
            while (cursor.getNext()) {
                final ByteIterable key = cursor.getKey();
                //noinspection ObjectAllocationInLoop
                if (isStringKey(new ArrayByteIterable(key))) {
                    if (TreeMetaInfo.load(env, cursor.getValue()).getStructureId() == structureId) {
                        return StringBinding.entryToString(key);
                    }
                }
            }
        }
        return null;
    }

    MetaTreeImpl getClone() {
        return new MetaTreeImpl(cloneTree(tree), root);
    }

    static boolean isStringKey(final ArrayByteIterable key) {
        // last byte of string is zero
        return key.byteAt(key.getLength() - 1) == 0;
    }

    static ITreeMutable cloneTree(@NotNull final ITree tree) {
        try (ITreeCursor cursor = tree.openCursor()) {
            final ITreeMutable result = tree.getMutableCopy();
            while (cursor.getNext()) {
                result.put(cursor.getKey(), cursor.getValue());
            }
            return result;
        }
    }

    private static ITree getEmptyMetaTree(@NotNull final EnvironmentImpl env) {
        return new BTreeEmpty(env.getLog(), env.getBTreeBalancePolicy(), false, EnvironmentImpl.META_TREE_ID) {
            @NotNull
            @Override
            public DataIterator getDataIterator(long address) {
                return new DataIterator(log, address);
            }
        };
    }

    static class Proto implements MetaTreePrototype {
        final long address;
        final long root;

        Proto(long address, long root) {
            this.address = address;
            this.root = root;
        }

        @Override
        public long treeAddress() {
            return address;
        }

        @Override
        public long rootAddress() {
            return root;
        }
    }
}
