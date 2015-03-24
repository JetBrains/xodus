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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.log.iterate.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.tree.*;
import jetbrains.exodus.tree.btree.BTree;
import jetbrains.exodus.tree.btree.BTreeEmpty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

final class MetaTree {

    final ITree tree;
    final long root;
    final long highAddress;

    MetaTree(final ITree tree, long root, long highAddress) {
        this.tree = tree;
        this.root = root;
        this.highAddress = highAddress;
    }

    static Pair<MetaTree, Integer> create(@NotNull final EnvironmentImpl env) {
        final Log log = env.getLog();
        DatabaseRoot dbRoot = (DatabaseRoot) log.getLastLoggableOfType(DatabaseRoot.DATABASE_ROOT_TYPE);
        int resultId = EnvironmentImpl.META_TREE_ID;
        ITree resultTree = null;
        while (dbRoot != null) {
            if (dbRoot.isValid()) {
                final long rootAddress = dbRoot.getRootAddress();
                final int structureId = dbRoot.getLastStructureId();
                final BTree treeCandidate = env.loadMetaTree(rootAddress);
                if (treeCandidate != null) {
                    resultId = structureId;
                    resultTree = treeCandidate;
                    break;
                }
            }
            // continue recovery
            dbRoot = (DatabaseRoot) log.getLastLoggableOfTypeBefore(DatabaseRoot.DATABASE_ROOT_TYPE, dbRoot.getAddress());
        }
        final long root;
        final long validHighAddress;
        if (dbRoot != null) {
            root = dbRoot.getAddress();
            validHighAddress = root + dbRoot.length();
            if (log.getHighAddress() != validHighAddress) {
                log.setHighAddress(validHighAddress);
            }
        } else {
            log.setHighAddress(0);
            resultTree = getEmptyMetaTree(env);
            final long rootAddress = resultTree.getMutableCopy().save();
            root = log.write(DatabaseRoot.toLoggable(rootAddress, EnvironmentImpl.META_TREE_ID));
            validHighAddress = root + log.read(root).length();
            log.flush();
        }
        return new Pair<MetaTree, Integer>(new MetaTree(resultTree, root, validHighAddress), resultId);
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
        final ITree rememberedTree = tree;
        final ByteIterable value = rememberedTree.get(LongBinding.longToCompressedEntry(structureId));
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
    @Nullable
    static MetaTree saveMetaTree(@NotNull final ITreeMutable metaTree,
                                 @NotNull final EnvironmentImpl env,
                                 @NotNull final Collection<Loggable> expired) {
        final long newMetaTreeAddress = metaTree.save();
        final Log log = env.getLog();
        final int lastStructureId = env.getLastStructureId();
        final long dbRootAddress = log.write(DatabaseRoot.toLoggable(newMetaTreeAddress, lastStructureId));
        log.flush();
        final BTree resultTree = env.loadMetaTree(newMetaTreeAddress);
        final RandomAccessLoggable dbRootLoggable = log.read(dbRootAddress);
        expired.add(dbRootLoggable);
        return new MetaTree(resultTree, dbRootAddress, dbRootAddress + dbRootLoggable.length());
    }

    @NotNull
    List<String> getAllStoreNames() {
        final ITree tree = this.tree;
        if (tree.getSize() == 0) {
            return Collections.emptyList();
        }
        final List<String> result = new ArrayList<String>();
        final ITreeCursor cursor = tree.openCursor();
        while (cursor.getNext()) {
            final ArrayByteIterable key = new ArrayByteIterable(cursor.getKey());
            if (isStringKey(key)) {
                final String storeName = StringBinding.entryToString(key);
                if (!EnvironmentImpl.isUtilizationProfile(storeName)) {
                    result.add(storeName);
                }
            }
        }
        return result;
    }

    @Nullable
    String getStoreNameByStructureId(final int structureId, @NotNull final EnvironmentImpl env) {
        final ITreeCursor cursor = tree.openCursor();
        try {
            while (cursor.getNext()) {
                final ByteIterable key = cursor.getKey();
                if (isStringKey(new ArrayByteIterable(key))) {
                    if (TreeMetaInfo.load(env, cursor.getValue()).getStructureId() == structureId) {
                        return StringBinding.entryToString(key);
                    }
                }
            }

        } finally {
            cursor.close();
        }
        return null;
    }

    MetaTree getClone() {
        final ITreeCursor cursor = tree.openCursor();
        try {
            final ITreeMutable tree = this.tree.getMutableCopy();
            while (cursor.getNext()) {
                tree.put(cursor.getKey(), cursor.getValue());
            }
            return new MetaTree(tree, root, highAddress);
        } finally {
            cursor.close();
        }
    }

    static boolean isStringKey(final ArrayByteIterable key) {
        // last byte of string is zero
        return key.getBytesUnsafe()[key.getLength() - 1] == 0;
    }

    private static ITree getEmptyMetaTree(@NotNull final EnvironmentImpl env) {
        return new BTreeEmpty(env.getLog(), env.getBTreeBalancePolicy(), false, EnvironmentImpl.META_TREE_ID);
    }
}
