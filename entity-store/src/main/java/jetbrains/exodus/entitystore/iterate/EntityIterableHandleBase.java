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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterableHandle;
import jetbrains.exodus.entitystore.EntityIterableType;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.UnsupportedEncodingException;

@SuppressWarnings({"AbstractClassWithoutAbstractMethods", "RawUseOfParameterizedType", "ProtectedField"})
public abstract class EntityIterableHandleBase implements EntityIterableHandle {

    private static final int HASH_LONGS_COUNT = 4; // NB: the fact that it is a power of 2 is used

    @Nullable
    protected final PersistentEntityStore store;
    @NotNull
    private final EntityIterableHandleHash hash;
    private int hashCode;
    private boolean hashCodeComputed;

    protected EntityIterableHandleBase(@Nullable final PersistentEntityStore store,
                                       @NotNull final EntityIterableType type) {
        this.store = store;
        hash = new EntityIterableHandleHash();
        hash.apply(type.getType());
        if (type != EntityIterableType.EMPTY) {
            hash.applyDelimiter();
        }
        hashCodeComputed = false;
    }

    @Override
    public boolean hasLinkId(int id) {
        final int[] linkIds = getLinkIds();
        if (linkIds == null)
            return false;
        int high = linkIds.length - 1;
        if (high == 0)
            return linkIds[0] == id;
        // copy-pasted Arrays.binarySearch
        int low = 0;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = linkIds[mid];

            if (midVal < id)
                low = mid + 1;
            else if (midVal > id)
                high = mid - 1;
            else
                return true;
        }
        return false;
    }

    @Nullable
    public PersistentEntityStore getStore() {
        return store;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof EntityIterableHandleBase)) {
            return false;
        }
        final EntityIterableHandleBase that = (EntityIterableHandleBase) obj;
        return store == that.store && hashCode() == that.hashCode() && hash.equals(that.hash);
    }

    public final int hashCode() {
        if (!hashCodeComputed) {
            hashCode(hash);
            hashCodeComputed = true;
            hashCode = hash.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean isMatchedEntityAdded(@NotNull final EntityId added) {
        return true;
    }

    @Override
    public boolean isMatchedEntityDeleted(@NotNull final EntityId deleted) {
        return true;
    }

    @Override
    public boolean isMatchedPropertyChanged(final int typeId,
                                            final int propertyId,
                                            @Nullable final Comparable oldValue,
                                            @Nullable final Comparable newValue) {
        return true;
    }

    @Nullable
    @Override
    public int[] getLinkIds() {
        return null;
    }

    @Override
    public boolean isExpired() {
        return false;
    }

    @Override
    public String toString() {
        return getHash().toString();
    }

    @NotNull
    public EntityIterableHandleHash getHash() {
        //noinspection ResultOfMethodCallIgnored
        hashCode(); // make sure hash is populated
        return hash;
    }

    protected abstract void hashCode(@NotNull final EntityIterableHandleHash hash);

    public static int[] mergeLinkIds(@Nullable final int[] left, @Nullable final int[] right) {
        if (left == null) return right;
        if (right == null) return left;
        final int l = left.length;
        final int r = right.length;
        final int mergedLength = getMergedLength(left, right, l, r);
        if (mergedLength == l) return left;
        if (mergedLength == r) return right;
        return merge(left, right, l, r, new int[mergedLength]);
    }

    private static int getMergedLength(int[] left, int[] right, int l, int r) {
        int i = 0, j = 0, k = 0;
        while (true) {
            final int a = left[i];
            final int b = right[j];
            if (a <= b) {
                i++;
                k++;
                if (i >= l) {
                    if (a == b) {
                        j++;
                    }
                    k += r - j;
                    break;
                }
                if (a == b) {
                    j++;
                } else {
                    continue;
                }
            } else {
                j++;
                k++;
            }
            if (j >= r) {
                k += l - i;
                break;
            }
        }
        return k;
    }

    private static int[] merge(int[] left, int[] right, int l, int r, int[] result) {
        int i = 0, j = 0, k = 0;
        int a = left[0];
        int b = right[0];
        while (true) {
            if (a <= b) {
                i++;
                result[k++] = a;
                if (i < l) {
                    boolean neq = a != b;
                    a = left[i];
                    if (neq) {
                        continue;
                    }
                    j++;
                } else {
                    if (a == b) {
                        j++;
                    }
                    while (j < r) {
                        result[k++] = right[j++];
                    }
                    break;
                }
            } else {
                j++;
                result[k++] = b;
            }
            if (j < r) {
                b = right[j];
            } else {
                while (i < l) {
                    result[k++] = left[i++];
                }
                break;
            }
        }
        return result;
    }

    public static final class EntityIterableHandleHash {

        private static final String[] INTS;

        static {
            INTS = new String[256];
            for (int i = 0; i < INTS.length; ++i) {
                INTS[i] = Integer.toString(i);
            }
        }

        @NotNull
        private final long[] hashLongs;
        private int bytesProcessed;

        public EntityIterableHandleHash() {
            hashLongs = new long[HASH_LONGS_COUNT];
        }

        @Override
        public int hashCode() {
            long result = 314159265358L;
            for (final long hl : hashLongs) {
                result ^= hl;
            }
            return ((int) result) ^ ((int) (result >>> 32));
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof EntityIterableHandleHash)) {
                return false;
            }
            long[] rightHashLongs = ((EntityIterableHandleHash) obj).hashLongs;
            for (int i = 0; i < hashLongs.length; ++i) {
                if (hashLongs[i] != rightHashLongs[i]) {
                    return false;
                }
            }
            return true;
        }

        public void apply(final byte b) {
            final int bytesProcessed = this.bytesProcessed;
            if (bytesProcessed < HASH_LONGS_COUNT * 8) {
                hashLongs[((bytesProcessed >> 3) & (HASH_LONGS_COUNT - 1))] += (((long) (b & 0xff)) << ((bytesProcessed & 7) << 3));
            } else {
                final int index = bytesProcessed & (HASH_LONGS_COUNT - 1);
                final long hashValue = hashLongs[index];
                hashLongs[index] = (hashValue << 5) - hashValue /* same as hashValue*31 */ + (b & 0xff);
            }
            this.bytesProcessed = bytesProcessed + 1;
        }

        public void apply(final int i) {
            if (i >= 0 && i < INTS.length) {
                apply(INTS[i]);
            } else {
                apply(Integer.toString(i));
            }
        }

        public void apply(final long l) {
            if (l >= 0 && l < (long) INTS.length) {
                apply(INTS[((int) l)]);
            } else {
                apply(Long.toString(l));
            }
        }

        public void apply(@NotNull final String s) {
            try {
                for (final byte b : s.getBytes("UTF-8")) {
                    apply(b);
                }
            } catch (UnsupportedEncodingException e) {
                throw ExodusException.toExodusException(e);
            }
        }

        public void apply(@NotNull final EntityIterableHandle source) {
            ((EntityIterableHandleBase) source).getHash().forEachByte(new ByteConsumer() {
                @Override
                public void accept(final byte b) {
                    apply(b);
                }
            });
        }

        public void applyDelimiter() {
            apply((byte) '-');
        }

        @Override
        public String toString() {
            final int hashBytes = Math.min(bytesProcessed, HASH_LONGS_COUNT * 8);
            final StringBuilder builder = new StringBuilder(hashBytes);
            forEachByte(new ByteConsumer() {
                @Override
                public void accept(final byte b) {
                    builder.append((char) b);
                }
            });
            return builder.toString();
        }

        private void forEachByte(@NotNull final ByteConsumer consumer) {
            final int hashBytes = Math.min(bytesProcessed, HASH_LONGS_COUNT * 8);
            long hashLong = 0;
            for (int i = 0; i < hashBytes; i++) {
                if ((i & 7) == 0) {
                    hashLong = hashLongs[i >> 3];
                }
                consumer.accept((byte) (hashLong & 0xff));
                hashLong >>= 8;
            }
        }

        private static interface ByteConsumer {

            void accept(final byte b);
        }
    }
}
