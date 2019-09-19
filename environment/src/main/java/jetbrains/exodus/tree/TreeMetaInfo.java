/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.tree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.btree.BTreeMetaInfo;
import jetbrains.exodus.tree.patricia.PatriciaMetaInfo;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

public abstract class TreeMetaInfo {

    public static final TreeMetaInfo EMPTY = new Empty(0);

    protected static final int DUPLICATES_BIT = 1;
    protected static final int KEY_PREFIXING_BIT = 2;

    public final boolean duplicates;
    public final int structureId;
    public final Log log;

    protected TreeMetaInfo(final Log log, final boolean duplicates, final int structureId) {
        this.log = log;
        this.duplicates = duplicates;
        this.structureId = structureId;
    }

    public boolean hasDuplicates() {
        return duplicates;
    }

    public abstract boolean isKeyPrefixing();

    public int getStructureId() {
        return structureId;
    }

    public ByteIterable toByteIterable() { // TODO: generify and extract BTree and Patricia-related stuff to methods
        byte flags = (byte) (duplicates ? DUPLICATES_BIT : 0);
        if (isKeyPrefixing()) {
            flags += KEY_PREFIXING_BIT;
        }
        final LightOutputStream output = new LightOutputStream(10);
        output.write(flags);
        CompressedUnsignedLongByteIterable.fillBytes(0, output); // legacy format
        CompressedUnsignedLongByteIterable.fillBytes(structureId, output);
        return output.asArrayByteIterable();
    }

    public abstract TreeMetaInfo clone(final int newStructureId);

    public static StoreConfig toConfig(@NotNull final TreeMetaInfo metaInfo) {
        if (metaInfo.getStructureId() < 0) {
            return StoreConfig.TEMPORARY_EMPTY;
        }
        return StoreConfig.getStoreConfig(metaInfo.duplicates, metaInfo.isKeyPrefixing());
    }

    public static TreeMetaInfo load(@NotNull final EnvironmentImpl environment,
                                    final boolean duplicates,
                                    final boolean keyPrefixing,
                                    final int structureId) {
        if (keyPrefixing) {
            return new PatriciaMetaInfo(environment.getLog(), duplicates, structureId);
        } else {
            return new BTreeMetaInfo(environment, duplicates, structureId);
        }
    }

    public static TreeMetaInfo load(@NotNull final EnvironmentImpl environment, @NotNull final ByteIterable iterable) {
        final ByteIterator it = iterable.iterator();
        final byte flagsByte = it.next();
        if ((flagsByte & KEY_PREFIXING_BIT) == 0) {
            return BTreeMetaInfo.load(environment, flagsByte, it);
        } else {
            return PatriciaMetaInfo.load(environment, flagsByte, it);
        }
    }

    @NotNull
    public static ExpiredLoggableCollection getTreeLoggables(@NotNull final ITree tree) {
        if (tree.getSize() > 100000) {
            return ExpiredLoggableCollection.getFROM_SCRATCH();
        }
        final ExpiredLoggableCollection result = new ExpiredLoggableCollection();
        final LongIterator it = tree.addressIterator();
        final Log log = tree.getLog();
        while (it.hasNext()) {
            final long nextAddress = it.next();
            result.add(log.readNotNull(tree.getDataIterator(nextAddress), nextAddress));
        }
        return result;
    }

    private static final class Empty extends TreeMetaInfo {
        private Empty(final int structureId) {
            super(null, false, structureId);
        }

        @Override
        public boolean isKeyPrefixing() {
            return false;
        }

        @Override
        public TreeMetaInfo clone(final int newStructureId) {
            return new Empty(newStructureId);
        }
    }
}
