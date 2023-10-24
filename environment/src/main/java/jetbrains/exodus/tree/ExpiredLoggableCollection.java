/*
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
package jetbrains.exodus.tree;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import org.jetbrains.annotations.NotNull;

import java.lang.UnsupportedOperationException;
import java.util.function.BiConsumer;

public interface ExpiredLoggableCollection {
    ExpiredLoggableCollection EMPTY = new EmptyLoggableCollection();

    int NON_ACCUMULATED_STATS_LIMIT = 1_000;

    int getSize();

    void add(Loggable loggable);

    void add(long address, int length);

    ExpiredLoggableCollection trimToSize();

    ExpiredLoggableCollection mergeWith(ExpiredLoggableCollection parent);

    void forEach(BiConsumer<Long, Integer> action);

    @NotNull
    static ExpiredLoggableCollection newInstance(Log log) {
        return new MutableExpiredLoggableCollection(log);
    }
}

class MutableExpiredLoggableCollection implements ExpiredLoggableCollection {
    private final Log log;
    private MutableExpiredLoggableCollection parent;
    @NotNull
    private final LongArrayList addresses;
    @NotNull
    private final IntArrayList lengths;
    private Long2IntOpenHashMap accumulatedStats;
    private int _size;

    public MutableExpiredLoggableCollection(Log log) {
        this(log, null, new LongArrayList(), new IntArrayList(), null, 0);
    }

    public MutableExpiredLoggableCollection(
            Log log,
            MutableExpiredLoggableCollection parent,
            @NotNull LongArrayList addresses,
            @NotNull IntArrayList lengths,
            Long2IntOpenHashMap accumulatedStats, int size) {
        this.log = log;
        this.parent = parent;
        this.addresses = addresses;
        this.lengths = lengths;
        this.accumulatedStats = accumulatedStats;
        this._size = size;
    }

    @Override
    public int getSize() {
        return _size + (parent != null ? parent.getSize() : 0);
    }

    private int getNonAccumulatedSize() {
        return lengths.size() + (parent != null ? parent.lengths.size() + (parent.accumulatedStats != null ? parent.accumulatedStats.size() : 0) : 0);
    }

    @Override
    public void add(Loggable loggable) {
        add(loggable.getAddress(), loggable.length());
    }

    @Override
    public void add(long address, int length) {
        addresses.add(address);
        lengths.add(length);

        _size++;

        accumulateStats();
    }

    private void accumulateStats() {
        if (getNonAccumulatedSize() >= NON_ACCUMULATED_STATS_LIMIT) {
            if (accumulatedStats == null) {
                accumulatedStats = new Long2IntOpenHashMap();
            }

            MutableExpiredLoggableCollection currentParent = parent;
            IntArrayList currentLengths = lengths;
            LongArrayList currentAddresses = addresses;
            Long2IntOpenHashMap currentAccumulatedStats = null;

            while (true) {
                for (int i = 0; i < currentLengths.size(); i++) {
                    long currentAddress = currentAddresses.getLong(i);
                    long currentFileAddress = log.getFileAddress(currentAddress);
                    int currentLength = currentLengths.getInt(i);

                    accumulatedStats.mergeInt(
                            currentFileAddress,
                            currentLength,
                            Integer::sum);
                }

                if (currentAccumulatedStats != null) {
                    for (Long2IntMap.Entry entry : currentAccumulatedStats.long2IntEntrySet()) {
                        accumulatedStats.mergeInt(
                                entry.getLongKey(),
                                entry.getIntValue(),
                                Integer::sum);
                    }
                }

                if (currentParent != null) {
                    currentLengths = currentParent.lengths;
                    currentAddresses = currentParent.addresses;
                    currentParent = currentParent.parent;
                    currentAccumulatedStats = currentParent != null ? currentParent.accumulatedStats : null;
                } else {
                    break;
                }
            }

            parent = null;

            addresses.clear();
            lengths.clear();
        }
    }

    @Override
    public ExpiredLoggableCollection trimToSize() {
        addresses.trim();
        lengths.trim();
        return this;
    }

    @Override
    public ExpiredLoggableCollection mergeWith(ExpiredLoggableCollection parent) {
        if (parent instanceof MutableExpiredLoggableCollection) {
            MutableExpiredLoggableCollection parentAsMutable = (MutableExpiredLoggableCollection) parent;
            return (this.parent != null ? new MutableExpiredLoggableCollection(log,
                    this, parentAsMutable.addresses, parentAsMutable.lengths, parentAsMutable.accumulatedStats,
                    parentAsMutable._size)
                    : new MutableExpiredLoggableCollection(log, parentAsMutable, addresses, lengths, accumulatedStats,
                    _size))
                    .applyAccumulateStats();
        }

        return this;
    }

    private MutableExpiredLoggableCollection applyAccumulateStats() {
        accumulateStats();
        return this;
    }

    @Override
    public void forEach(BiConsumer<Long, Integer> action) {
        MutableExpiredLoggableCollection current = this;

        while (current != null) {
            for (int i = 0; i < current.lengths.size(); i++) {
                action.accept(current.addresses.getLong(i), current.lengths.getInt(i));
            }

            if (current.accumulatedStats != null) {
                for (Long2IntMap.Entry entry : current.accumulatedStats.long2IntEntrySet()) {
                    action.accept(entry.getLongKey(), entry.getIntValue());
                }
            }

            current = current.parent;
        }
    }

    @Override
    public String toString() {
        return "Expired " + getSize() + " loggables";
    }
}

class EmptyLoggableCollection implements ExpiredLoggableCollection {
    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public void add(Loggable loggable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(long address, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExpiredLoggableCollection trimToSize() {
        return this;
    }

    @Override
    public ExpiredLoggableCollection mergeWith(ExpiredLoggableCollection parent) {
        if (parent.getSize() > 0) {
            throw new UnsupportedOperationException();
        }

        return this;
    }

    @Override
    public void forEach(BiConsumer<Long, Integer> action) {
    }
}

