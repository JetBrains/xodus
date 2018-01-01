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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.tree.TreeCursor;
import org.jetbrains.annotations.NotNull;

public class PatriciaTree extends PatriciaTreeBase {

    private final RandomAccessLoggable rootLoggable;
    private final ImmutableNode root;

    public PatriciaTree(@NotNull final Log log, final long rootAddress, final int structureId) {
        super(log, structureId);
        if (rootAddress == Loggable.NULL_ADDRESS) {
            throw new IllegalArgumentException("Can't instantiate nonempty tree with null root address");
        }
        rootLoggable = getLoggable(rootAddress);
        final byte type = rootLoggable.getType();
        if (!nodeIsRoot(type)) {
            throw new ExodusException("Unexpected root page type: " + type);
        }
        final ByteIterableWithAddress data = rootLoggable.getData();
        final ByteIteratorWithAddress it = data.iterator();
        size = CompressedUnsignedLongByteIterable.getLong(it);
        if (nodeHasBackReference(type)) {
            long backRef = CompressedUnsignedLongByteIterable.getLong(it);
            rememberBackRef(backRef);
        }
        root = new ImmutableNode(rootAddress, type, data.clone((int) (it.getAddress() - data.getDataAddress())));
    }

    @NotNull
    @Override
    public final PatriciaTreeMutable getMutableCopy() {
        return new PatriciaTreeMutable(log, structureId, size, getRoot());
    }

    @Override
    public final long getRootAddress() {
        return rootLoggable.getAddress();
    }

    @Override
    public final ITreeCursor openCursor() {
        final ImmutableNode root = getRoot();
        return new TreeCursor(new PatriciaTraverser(this, root), root.hasValue());
    }

    void rememberBackRef(final long backRef) {
        // do nothing
    }

    @Override
    final ImmutableNode getRoot() {
        return root;
    }
}
