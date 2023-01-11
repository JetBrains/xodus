/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeCursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static jetbrains.exodus.tree.patricia.PatriciaTreeWithDuplicates.getEscapedKeyValue;

public class PatriciaCursorDecorator implements ITreeCursor {
    private static final int UNKNOWN = -1;

    ITreeCursor patriciaCursor;

    ByteIterable keyBytes;
    int keyLength;
    int valueLength;

    ByteIterable nextKeyBytes;
    int nextKeyLength = UNKNOWN;
    int nextValueLength;

    ByteIterable prevKeyBytes;
    int prevKeyLength = UNKNOWN;
    int prevValueLength;

    public PatriciaCursorDecorator(ITreeCursor patriciaCursor) {
        this.patriciaCursor = patriciaCursor;
    }

    @Override
    public boolean isMutable() {
        return patriciaCursor.isMutable();
    }

    @Override
    public ITree getTree() {
        return patriciaCursor.getTree();
    }

    @Override
    public boolean getNext() {
        if (getNextLazy()) {
            advance();
            return true;
        }
        return false;
    }

    @Override
    public boolean getNextDup() {
        if (keyBytes == null) {
            throw new IllegalStateException("Cursor is not yet initialized");
        }
        if (getNextLazy() && keyBytes.compareTo(keyLength, nextKeyBytes, nextKeyLength) == 0) {
            advance();
            return true;
        }
        return false;
    }

    @Override
    public boolean getNextNoDup() {
        if (keyBytes == null) {
            return getNext(); // init
        }
        if (getNextLazy() && keyBytes.compareTo(keyLength, nextKeyBytes, nextKeyLength) != 0) {
            advance();
            return true;
        }
        // we must create new cursor 'cause we don't know if next "no dup" pair exists
        final ITreeCursor cursor = patriciaCursor.getTree().openCursor();
        ITreeCursor cursorToClose = cursor;
        try {
            if (cursor.getSearchKeyRange(getEscapedKeyValue(getKey(), getValue())) != null) {
                while (cursor.getNext()) {
                    final ByteIterable keyLengthIterable = cursor.getValue();
                    final ByteIterable noDupKey = new ArrayByteIterable(new UnEscapingByteIterable(cursor.getKey()));
                    final int keyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
                    if (keyBytes.compareTo(this.keyLength, noDupKey, keyLength) != 0) {
                        keyBytes = noDupKey;
                        this.keyLength = keyLength;
                        valueLength = noDupKey.getLength() - keyLength - 1;
                        cursorToClose = patriciaCursor;
                        patriciaCursor = cursor;
                        nextKeyLength = UNKNOWN; // forget computed next pair
                        prevKeyLength = UNKNOWN; // forget computed prev pair
                        return true;
                    }
                }
            }
        } finally {
            cursorToClose.close();
        }
        return false;
    }

    @Override
    public boolean getPrev() {
        if (getPrevLazy()) {
            retreat();
            return true;
        }
        return false;
    }

    @Override
    public boolean getPrevDup() {
        if (keyBytes == null) {
            throw new IllegalStateException("Cursor is not yet initialized");
        }
        if (getPrevLazy() && keyBytes.compareTo(keyLength, prevKeyBytes, prevKeyLength) == 0) {
            retreat();
            return true;
        }
        return false;
    }

    @Override
    public boolean getPrevNoDup() {
        if (keyBytes == null) {
            return getPrev(); // init
        }
        if (getPrevLazy() && keyBytes.compareTo(keyLength, prevKeyBytes, prevKeyLength) != 0) {
            advance();
            return true;
        }
        // we must create new cursor 'cause we don't know if prev "no dup" pair exists
        final ITreeCursor cursor = patriciaCursor.getTree().openCursor();
        ITreeCursor cursorToClose = cursor;
        try {
            if (cursor.getSearchKeyRange(getEscapedKeyValue(getKey(), getValue())) != null) {
                while (cursor.getPrev()) {
                    final ByteIterable keyLengthIterable = cursor.getValue();
                    final ByteIterable noDupKey = new ArrayByteIterable(new UnEscapingByteIterable(cursor.getKey()));
                    final int keyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
                    if (keyBytes.compareTo(this.keyLength, noDupKey, keyLength) != 0) {
                        keyBytes = noDupKey;
                        this.keyLength = keyLength;
                        valueLength = noDupKey.getLength() - keyLength - 1;
                        cursorToClose = patriciaCursor;
                        patriciaCursor = cursor;
                        prevKeyLength = UNKNOWN; // forget computed prev pair
                        nextKeyLength = UNKNOWN; // forget computed next pair
                        return true;
                    }
                }
            }
        } finally {
            cursorToClose.close();
        }
        return false;
    }

    @Override
    public boolean getLast() {
        if (patriciaCursor.getLast()) {
            final ByteIterable keyLengthIterable = patriciaCursor.getValue();
            final ByteIterable sourceKey = new ArrayByteIterable(new UnEscapingByteIterable(patriciaCursor.getKey()));
            keyBytes = sourceKey;
            keyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
            valueLength = sourceKey.getLength() - keyLength - 1;
            nextKeyLength = UNKNOWN; // forget computed next pair
            prevKeyLength = UNKNOWN; // forget computed prev pair
            return true;
        }
        return false;
    }

    @NotNull
    @Override
    public ByteIterable getKey() {
        return keyBytes == null ? ByteIterable.EMPTY : keyBytes.subIterable(0, keyLength);
    }

    @NotNull
    @Override
    public ByteIterable getValue() {
        if (keyBytes == null) {
            return ByteIterable.EMPTY;
        }
        final int offset = keyLength + 1;

        return keyBytes.subIterable(offset, valueLength);
    }

    @Nullable
    @Override
    public ByteIterable getSearchKey(@NotNull ByteIterable key) {
        final ITreeCursor cursor = patriciaCursor.getTree().openCursor();
        ITreeCursor cursorToClose = cursor;
        try {
            ByteIterable keyLengthIterable = cursor.getSearchKeyRange(new EscapingByteIterable(key));
            if (keyLengthIterable != null) {
                int keyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
                if (key.getLength() == keyLength) {
                    final ByteIterable noDupKey = new ArrayByteIterable(new UnEscapingByteIterable(cursor.getKey()));
                    if (key.compareTo(keyLength, noDupKey, keyLength) == 0) {
                        this.keyBytes = noDupKey;
                        this.keyLength = keyLength;
                        valueLength = noDupKey.getLength() - keyLength - 1;
                        cursorToClose = patriciaCursor;
                        patriciaCursor = cursor;
                        nextKeyLength = UNKNOWN; // forget computed next pair
                        prevKeyLength = UNKNOWN; // forget computed prev pair
                        return getValue();
                    }
                }
            }
        } finally {
            cursorToClose.close();
        }
        return null;
    }

    @Nullable
    @Override
    public ByteIterable getSearchKeyRange(@NotNull ByteIterable key) {
        final ByteIterable keyLengthIterable = patriciaCursor.getSearchKeyRange(new EscapingByteIterable(key));
        if (keyLengthIterable == null) {
            return null;
        }
        final ByteIterable noDupKey = new ArrayByteIterable(new UnEscapingByteIterable(patriciaCursor.getKey()));
        keyBytes = noDupKey;
        keyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
        valueLength = noDupKey.getLength() - keyLength - 1;
        nextKeyLength = UNKNOWN; // forget computed next pair
        prevKeyLength = UNKNOWN; // forget computed prev pair
        return getValue();
    }

    @Override
    public boolean getSearchBoth(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        final ITreeCursor cursor = patriciaCursor.getTree().openCursor();
        ITreeCursor cursorToClose = cursor;
        try {
            ByteIterable keyLengthIterable = cursor.getSearchKey(getEscapedKeyValue(key, value));
            if (keyLengthIterable == null) {
                return false;
            }
            final int keyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
            if (keyLength == key.getLength()) {
                keyBytes = new ArrayByteIterable(new UnEscapingByteIterable(cursor.getKey()));
                this.keyLength = keyLength;
                valueLength = value.getLength();
                cursorToClose = patriciaCursor;
                patriciaCursor = cursor;
                nextKeyLength = UNKNOWN; // forget computed next pair
                prevKeyLength = UNKNOWN; // forget computed prev pair
                return true;
            }
            return false;
        } finally {
            cursorToClose.close();
        }
    }

    @Nullable
    @Override
    public ByteIterable getSearchBothRange(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        final ITreeCursor cursor = patriciaCursor.getTree().openCursor();
        ITreeCursor cursorToClose = cursor;
        try {
            ByteIterable keyLengthIterable = cursor.getSearchKeyRange(new EscapingByteIterable(key));
            if (keyLengthIterable != null) {
                final int srcKeyLength = key.getLength();
                final int valueLength = value.getLength();
                while (true) {
                    int keyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
                    if (srcKeyLength == keyLength) {
                        final ByteIterable noDupKey = new ArrayByteIterable(new UnEscapingByteIterable(cursor.getKey()));
                        if (key.compareTo(keyLength, noDupKey, keyLength) == 0) {
                            // skip separator
                            final int noDupKeyLength = noDupKey.getLength() - keyLength - 1;
                            final int cmp = noDupKey.compareTo(keyLength + 1,
                                    keyLength + 1 + noDupKeyLength, value, 0, valueLength);
                            if (cmp >= 0) {
                                keyBytes = noDupKey;
                                this.keyLength = keyLength;
                                this.valueLength = noDupKeyLength;
                                cursorToClose = patriciaCursor;
                                patriciaCursor = cursor;
                                nextKeyLength = UNKNOWN; // forget computed next pair
                                prevKeyLength = UNKNOWN; // forget computed prev pair
                                return getValue();
                            }
                        }
                    }
                    if (cursor.getNext()) {
                        keyLengthIterable = cursor.getValue();
                    } else {
                        break;
                    }
                }
            }
        } finally {
            cursorToClose.close();
        }
        return null;
    }

    @Override
    public int count() {
        int result = 0;
        try (ITreeCursor cursor = patriciaCursor.getTree().openCursor()) {
            @Nullable
            ByteIterable value = cursor.getSearchKeyRange(new EscapingByteIterable(getKey()));
            while (value != null) {
                if (keyLength != CompressedUnsignedLongByteIterable.getInt(value)) {
                    break;
                }
                final ByteIterable noDupKey = new ArrayByteIterable(new UnEscapingByteIterable(cursor.getKey()));
                if (keyBytes.compareTo(keyLength, noDupKey, keyLength) != 0) {
                    break;
                }
                ++result;
                value = cursor.getNext() ? cursor.getValue() : null;
            }
        }
        return result;
    }

    @Override
    public void close() {
        patriciaCursor.close();
    }

    @Override
    public boolean deleteCurrent() {
        return patriciaCursor.deleteCurrent();
    }

    private void advance() {
        prevKeyBytes = keyBytes;
        prevKeyLength = keyLength;
        prevValueLength = valueLength;
        keyBytes = nextKeyBytes;
        keyLength = nextKeyLength;
        valueLength = nextValueLength;
        nextKeyLength = UNKNOWN; // forget computed next pair
    }

    private boolean getNextLazy() {
        if (nextKeyLength < 0) { // UNKNOWN
            if (patriciaCursor.getNext()) {
                final ByteIterable keyLengthIterable = patriciaCursor.getValue();
                final ByteIterable noDupKey = new ArrayByteIterable(new UnEscapingByteIterable(patriciaCursor.getKey()));
                nextKeyBytes = noDupKey;
                nextKeyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
                nextValueLength = noDupKey.getLength() - nextKeyLength - 1;
                return true;
            }
            return false;
        }
        return nextKeyBytes != null;
    }

    private void retreat() {
        nextKeyBytes = keyBytes;
        nextKeyLength = keyLength;
        nextValueLength = valueLength;
        keyBytes = prevKeyBytes;
        keyLength = prevKeyLength;
        valueLength = prevValueLength;
        prevKeyLength = UNKNOWN; // forget computed prev pair
    }

    private boolean getPrevLazy() {
        if (prevKeyLength < 0) { // UNKNOWN
            if (patriciaCursor.getPrev()) {
                final ByteIterable keyLengthIterable = patriciaCursor.getValue();
                final ByteIterable noDupKey = new ArrayByteIterable(new UnEscapingByteIterable(patriciaCursor.getKey()));
                prevKeyBytes = noDupKey;
                prevKeyLength = CompressedUnsignedLongByteIterable.getInt(keyLengthIterable);
                prevValueLength = noDupKey.getLength() - prevKeyLength - 1;
                return true;
            }
            return false;
        }
        return prevKeyBytes != null;
    }
}
