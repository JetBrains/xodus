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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterableHandle;
import jetbrains.exodus.entitystore.EntityIterableType;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import jetbrains.exodus.util.StringInterner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"AbstractClassWithoutAbstractMethods", "RawUseOfParameterizedType", "ProtectedField"})
public abstract class EntityIterableHandleBase implements EntityIterableHandle {

    @Nullable
    protected final PersistentEntityStore store;
    @NotNull
    protected final EntityIterableType type;
    private String cachedStringHandle;
    private int storeIdLength;

    protected EntityIterableHandleBase(@Nullable final PersistentEntityStore store,
                                       @NotNull final EntityIterableType type) {
        this.store = store;
        this.type = type;
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

    @Override
    @NotNull
    public EntityIterableType getType() {
        return type;
    }

    @Override
    @NotNull
    public String getStringHandle() {
        if (cachedStringHandle == null) {
            final StringBuilder builder = new StringBuilder(20);
            builder.append(store == null ? 0 : store.hashCode());
            builder.append('-');
            storeIdLength = builder.length();
            getStringHandle(builder);
            cachedStringHandle = StringInterner.intern(builder, 100);
        }
        return cachedStringHandle;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof EntityIterableHandleBase)) {
            return false;
        }
        EntityIterableHandle that = (EntityIterableHandle) obj;
        return getStringHandle().equals(that.getStringHandle());
    }

    public int hashCode() {
        return getStringHandle().hashCode();
    }

    @Override
    public void getStringHandle(@NotNull final StringBuilder builder) {
        builder.append(type.getType());
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

    @Nullable
    public String getCachedStringHandle() {
        String csh = cachedStringHandle;
        return csh == null ? null : csh.substring(storeIdLength);
    }

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
}
