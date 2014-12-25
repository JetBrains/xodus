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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.*;
import jetbrains.exodus.log.iterate.CompressedUnsignedLongByteIterable;
import org.jetbrains.annotations.NotNull;

public class BTree extends BTreeBase {

    private final RandomAccessLoggable rootLoggable;
    private final byte rootType;
    private final byte dataOffset;

    public BTree(@NotNull final Log log, final long rootAddress, final boolean allowsDuplicates, final int structureId) {
        this(log, rootAddress, BTreeBalancePolicy.DEFAULT, allowsDuplicates, structureId);
    }

    public BTree(@NotNull final Log log,
                 final long rootAddress,
                 @NotNull final BTreeBalancePolicy policy,
                 final boolean allowsDuplicates,
                 final int structureId) {
        super(policy, log, allowsDuplicates, structureId);
        if (rootAddress == Loggable.NULL_ADDRESS) {
            throw new IllegalArgumentException("Can't instantiate not empty tree with null root address.");
        }
        rootLoggable = getLoggable(rootAddress);
        final byte type = rootLoggable.getType();
        // load size, but check if it exists
        if (type != BOTTOM_ROOT && type != INTERNAL_ROOT) {
            throw new ExodusException("Unexpected root page type: " + type);
        }
        rootType = type;
        final ByteIterableWithAddress data = rootLoggable.getData();
        final ByteIteratorWithAddress it = data.iterator();
        size = CompressedUnsignedLongByteIterable.getLong(it);
        dataOffset = (byte) (it.getAddress() - data.getDataAddress());
    }

    @Override
    public long getRootAddress() {
        return rootLoggable.getAddress();
    }

    @Override
    @NotNull
    public BTreeMutable getMutableCopy() {
        final BTreeMutable result = new BTreeMutable(
                getBalancePolicy(), getLog(), getStructureId(), allowsDuplicates, this);
        result.addExpiredLoggable(rootLoggable);
        return result;
    }

    @Override
    @NotNull
    protected BasePage getRoot() {
        return loadPage(rootType, rootLoggable.getData().clone(dataOffset));
    }

}
