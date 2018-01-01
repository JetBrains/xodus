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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.SingleByteIterable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.tree.ITreeMutable;
import jetbrains.exodus.tree.LongIterator;
import jetbrains.exodus.util.ByteIterableUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public class PatriciaTreeWithDuplicates extends PatriciaTreeDecorator {

    public PatriciaTreeWithDuplicates(@NotNull final Log log, final long rootAddress, final int structureId) {
        this(log, rootAddress, structureId, false);
    }

    public PatriciaTreeWithDuplicates(@NotNull final Log log,
                                      final long rootAddress,
                                      final int structureId,
                                      final boolean empty) {
        super(empty ? new PatriciaTreeEmpty(log, structureId, false) : new PatriciaTree(log, rootAddress, structureId));
    }

    protected PatriciaTreeWithDuplicates(@NotNull final ITree treeNoDuplicates) {
        super(treeNoDuplicates);
    }

    @Nullable
    @Override
    public ByteIterable get(@NotNull final ByteIterable key) {
        try (ITreeCursor cursor = treeNoDuplicates.openCursor()) {
            final ByteIterable value = cursor.getSearchKeyRange(getEscapedKeyWithSeparator(key));
            if (value != null && value != ByteIterable.EMPTY) {
                int keyLength = CompressedUnsignedLongByteIterable.getInt(value);
                if (key.getLength() == keyLength) {
                    final ByteIterable noDupKey = new UnEscapingByteIterable(cursor.getKey());
                    final byte[] noDupKeyBytes = noDupKey.getBytesUnsafe();
                    if (ByteIterableUtil.compare(key.getBytesUnsafe(), keyLength, noDupKeyBytes, keyLength) == 0) {
                        return new ArrayByteIterable(Arrays.copyOfRange(noDupKeyBytes,
                                keyLength + 1, // skip separator
                                noDupKey.getLength()));
                    }
                }
            }
            return null;
        }
    }

    @Override
    public boolean hasPair(@NotNull final ByteIterable key, final @NotNull ByteIterable value) {
        return treeNoDuplicates.hasKey(getEscapedKeyValue(key, value));
    }

    @NotNull
    @Override
    public ITreeMutable getMutableCopy() {
        return new PatriciaTreeWithDuplicatesMutable(treeNoDuplicates.getMutableCopy());
    }

    @Override
    public ITreeCursor openCursor() {
        return new PatriciaCursorDecorator(treeNoDuplicates.openCursor());
    }

    @Override
    public LongIterator addressIterator() {
        return treeNoDuplicates.addressIterator();
    }

    protected static ByteIterable getEscapedKeyValue(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return new CompoundByteIterable(new ByteIterable[]{
                new EscapingByteIterable(key),
                SingleByteIterable.getIterable((byte) 0),
                new EscapingByteIterable(value)
        });
    }

    protected static ByteIterable getEscapedKeyWithSeparator(@NotNull final ByteIterable key) {
        return new CompoundByteIterable(new ByteIterable[]{
                new EscapingByteIterable(key),
                SingleByteIterable.getIterable((byte) 0)
        });
    }
}
