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
package jetbrains.exodus.entitystore.iterate;

import org.jetbrains.annotations.NotNull;

interface IdFilter {

    int[] EMPTY_ID_ARRAY = new int[0];

    boolean hasId(int id);
}

final class TrivialNegativeIdFilter implements IdFilter {

    static final IdFilter INSTANCE = new TrivialNegativeIdFilter();

    private TrivialNegativeIdFilter() {
    }

    @Override
    public boolean hasId(final int id) {
        return false;
    }
}

abstract class InitialIdFilter implements IdFilter {

    @Override
    public boolean hasId(final int id) {
        final int[] ids = getIds();
        final int idCount = ids.length;
        if (idCount == 0) {
            setFinalIdFilter(TrivialNegativeIdFilter.INSTANCE);
            return false;
        }
        if (idCount == 1) {
            final int singleId = ids[0];
            setFinalIdFilter(new SingleIdFilter(singleId));
            return singleId == id;
        }
        final BloomIdFilter finalFilter = idCount < 4 ? new LinearSearchIdFilter(ids) : new BinarySearchIdFilter(ids);
        setFinalIdFilter(finalFilter);
        return finalFilter.hasId(id);
    }

    abstract int[] getIds();

    abstract void setFinalIdFilter(@NotNull IdFilter filter);
}

final class SingleIdFilter implements IdFilter {

    private final int id;

    SingleIdFilter(final int id) {
        this.id = id;
    }

    @Override
    public boolean hasId(int id) {
        return id == this.id;
    }
}

abstract class BloomIdFilter implements IdFilter {

    final int[] ids;
    private final int bloomFilter;

    BloomIdFilter(@NotNull final int[] ids) {
        this.ids = ids;
        int bloomFilter = 0;
        for (int id : ids) {
            bloomFilter |= (1 << (id & 0x1f));
        }
        this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean hasId(final int id) {
        return (bloomFilter & (1 << (id & 0x1f))) != 0;
    }
}

final class LinearSearchIdFilter extends BloomIdFilter {

    LinearSearchIdFilter(@NotNull final int[] ids) {
        super(ids);
    }

    @Override
    public boolean hasId(final int id) {
        if (super.hasId(id)) {
            for (int i : ids) {
                if (i == id) {
                    return true;
                }
                if (i > id) {
                    break;
                }
            }
        }
        return false;
    }
}

final class BinarySearchIdFilter extends BloomIdFilter {

    BinarySearchIdFilter(@NotNull final int[] ids) {
        super(ids);
    }

    @Override
    public boolean hasId(final int id) {
        if (super.hasId(id)) {
            // copy-pasted Arrays.binarySearch
            int high = ids.length - 1;
            int low = 0;
            while (low <= high) {
                final int mid = (low + high) >>> 1;
                final int midVal = ids[mid];
                if (midVal < id) {
                    low = mid + 1;
                } else if (midVal > id) {
                    high = mid - 1;
                } else {
                    return true;
                }
            }
        }
        return false;
    }
}