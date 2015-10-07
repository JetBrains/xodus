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
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.RandomAccessLoggable;
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
        if (dbRoot != null) {
            do {
                final long root = dbRoot.getAddress();
                if (dbRoot.isValid()) {
                    try {
                        final long validHighAddress = root + dbRoot.length();
                        if (log.getHighAddress() != validHighAddress) {
                            log.setHighAddress(validHighAddress);
                        }
                        final BTree metaTree = env.loadMetaTree(dbRoot.getRootAddress());
                        if (metaTree != null) {
                            cloneTree(metaTree); // try to traverse meta tree
                            return new Pair<>(new MetaTree(metaTree, root, validHighAddress), dbRoot.getLastStructureId());
                        }
                    } catch (ExodusException ignore) {
                        // XD-449: try next database root if we failed to traverse whole MetaTree
                        // TODO: this check should become obsolete after XD-334 is implemented
                    }
                }
                // continue recovery
                dbRoot = (DatabaseRoot) log.getLastLoggableOfTypeBefore(DatabaseRoot.DATABASE_ROOT_TYPE, root);
            } while (dbRoot != null);

            // "abnormal program termination", "blue screen of doom"
            // Something quite strange with the database: it is not empty, but no valid
            // root has found. We can't just reset the database and lose all the contents,
            // we should have a chance to investigate the case. So failing...
            throw new ExodusException("Database is not empty, but no valid root found");

        }
        // no roots found: the database is empty
        log.setHighAddress(0);
        final ITree resultTree = getEmptyMetaTree(env);
        final long rootAddress = resultTree.getMutableCopy().save();
        final long root = log.write(DatabaseRoot.toLoggable(rootAddress, EnvironmentImpl.META_TREE_ID));
        log.flush();
        return new Pair<>(new MetaTree(resultTree, root, log.getHighAddress()), EnvironmentImpl.META_TREE_ID);
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
        final List<String> result = new ArrayList<>();
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
        try (ITreeCursor cursor = tree.openCursor()) {
            while (cursor.getNext()) {
                final ByteIterable key = cursor.getKey();
                if (isStringKey(new ArrayByteIterable(key))) {
                    if (TreeMetaInfo.load(env, cursor.getValue()).getStructureId() == structureId) {
                        return StringBinding.entryToString(key);
                    }
                }
            }
        }
        return null;
    }

    MetaTree getClone() {
        return new MetaTree(cloneTree(tree), root, highAddress);
    }

    static boolean isStringKey(final ArrayByteIterable key) {
        // last byte of string is zero
        return key.getBytesUnsafe()[key.getLength() - 1] == 0;
    }

    private static ITreeMutable cloneTree(@NotNull final ITree tree) {
        try (ITreeCursor cursor = tree.openCursor()) {
            final ITreeMutable result = tree.getMutableCopy();
            while (cursor.getNext()) {
                result.put(cursor.getKey(), cursor.getValue());
            }
            return result;
        }
    }

    private static ITree getEmptyMetaTree(@NotNull final EnvironmentImpl env) {
        return new BTreeEmpty(env.getLog(), env.getBTreeBalancePolicy(), false, EnvironmentImpl.META_TREE_ID);
    }
}
