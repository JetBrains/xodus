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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.RandomAccessByteIterable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.log.iterate.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.tree.TreeCursor;
import org.jetbrains.annotations.NotNull;

public class PatriciaTree extends PatriciaTreeBase {

    private final long rootAddress;
    private final int rootType;
    private final int dataOffset;

    public PatriciaTree(@NotNull final Log log,
                        final long rootAddress,
                        final long structureId) {
        super(log, structureId);
        if (rootAddress == Loggable.NULL_ADDRESS) {
            throw new IllegalArgumentException("Can't instantiate nonempty tree with null root address");
        }
        final RandomAccessLoggable rootLoggable = getLoggable(rootAddress);
        final int type = rootLoggable.getType();
        if (!nodeIsRoot(type)) {
            throw new ExodusException("Unexpected root page type: " + type);
        }
        final RandomAccessByteIterable data = rootLoggable.getData();
        final ByteIterator itr = data.iterator();
        size = CompressedUnsignedLongByteIterable.getLong(itr);
        int offset = CompressedUnsignedLongByteIterable.getCompressedSize(size);
        if (nodeHasBackReference(type)) {
            long backRef = CompressedUnsignedLongByteIterable.getLong(itr);
            rememberBackRef(backRef);
            offset += CompressedUnsignedLongByteIterable.getCompressedSize(backRef);
        }
        this.rootAddress = rootAddress;
        rootType = type;
        dataOffset = offset + rootLoggable.getHeaderLength();
    }

    @NotNull
    @Override
    public final PatriciaTreeMutable getMutableCopy() {
        return new PatriciaTreeMutable(log, structureId, size, getRoot());
    }

    @Override
    public final long getRootAddress() {
        return rootAddress;
    }

    @Override
    public final ITreeCursor openCursor() {
        final ImmutableNode root = getRoot();
        return new TreeCursor(new PatriciaTraverser(root), root.hasValue());
    }

    void rememberBackRef(final long backRef) {
        // do nothing
    }

    @Override
    final ImmutableNode getRoot() {
        return new ImmutableNode(this, rootAddress, rootType, new RandomAccessByteIterable(rootAddress + dataOffset, log));
    }
}
