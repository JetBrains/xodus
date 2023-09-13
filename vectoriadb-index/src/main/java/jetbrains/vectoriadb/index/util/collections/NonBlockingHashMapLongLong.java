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
/*
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
package jetbrains.vectoriadb.index.util.collections;

import jetbrains.vectoriadb.index.hash.XxHash;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;


/**
 * A lock-free alternate implementation of {@link java.util.concurrent.ConcurrentHashMap}
 * with <strong>primitive long keys</strong>, better scaling properties and
 * generally lower costs.  The use of {@code long} keys allows for faster
 * compares and lower memory costs.  The Map provides identical correctness
 * properties as ConcurrentHashMap.  All operations are non-blocking and
 * multi-thread safe, including all update operations.  {@link
 * NonBlockingHashMapLongLong} scales substatially better than {@link
 * java.util.concurrent.ConcurrentHashMap} for high update rates, even with a large
 * concurrency factor.  Scaling is linear up to 768 CPUs on a 768-CPU Azul
 * box, even with 100% updates or 100% reads or any fraction in-between.
 * Linear scaling up to all cpus has been observed on a 32-way Sun US2 box,
 * 32-way Sun Niagra box, 8-way Intel box and a 4-way Power box.
 *
 * <p><strong>The main benefit of this class</strong> over using plain {@link
 * org.jctools.maps.NonBlockingHashMap} with {@link Long} keys is
 * that it avoids the auto-boxing and unboxing costs.  Since auto-boxing is
 * <em>automatic</em>, it is easy to accidentally cause auto-boxing and negate
 * the space and speed benefits.
 *
 * <p>This class obeys the same functional specification as {@link
 * java.util.Hashtable}, and includes versions of methods corresponding to
 * each method of <tt>Hashtable</tt>.  However, even though all operations are
 * thread-safe, operations do <em>not</em> entail locking and there is
 * <em>not</em> any support for locking the entire table in a way that
 * prevents all access.  This class is fully interoperable with
 * <tt>Hashtable</tt> in programs that rely on its thread safety but not on
 * its synchronization details.
 *
 * <p> Operations (including <tt>put</tt>) generally do not block, so may
 * overlap with other update operations (including other <tt>puts</tt> and
 * <tt>removes</tt>).  Retrievals reflect the results of the most recently
 * <em>completed</em> update operations holding upon their onset.  For
 * aggregate operations such as <tt>putAll</tt>, concurrent retrievals may
 * reflect insertion or removal of only some entries.  Similarly, Iterators
 * and Enumerations return elements reflecting the state of the hash table at
 * some point at or since the creation of the iterator/enumeration.  They do
 * <em>not</em> throw {@link ConcurrentModificationException}.  However,
 * iterators are designed to be used by only one thread at a time.
 *
 * <p> Very full tables, or tables with high re-probe rates may trigger an
 * internal resize operation to move into a larger table.  Resizing is not
 * terribly expensive, but it is not free either; during resize operations
 * table throughput may drop somewhat.  All threads that visit the table
 * during a resize will 'help' the resizing but will still be allowed to
 * complete their operation before the resize is finished (i.e., a simple
 * 'lockForRead' operation on a million-entry table undergoing resizing will not need
 * to block until the entire million entries are copied).
 *
 * @author Cliff Click
 * @since 1.5
 */

public class NonBlockingHashMapLongLong {
    private static final VarHandle CHM_HANDLE;

    static {
        try {
            CHM_HANDLE = MethodHandles
                    .privateLookupIn(NonBlockingHashMapLongLong.class, MethodHandles.lookup())
                    .findVarHandle(NonBlockingHashMapLongLong.class, "_chm", CHM.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final int REPROBE_LIMIT = 10; // Too many reprobes then force a table-resize

    // --- The Hash Table --------------------
    private CHM _chm;

    // Time since last resize
    private long _last_resize_milli;

    // Optimize for space: use a 1/2-sized table and allow more re-probes
    private final boolean _opt_for_space;

    // --- Minimum table size ----------------
    // Pick size 16 K/V pairs, which turns into (16*2)*4+12 = 140 bytes on a
    // standard 32-bit HotSpot, and (16*2)*8+12 = 268 bytes on 64-bit Azul.
    private static final int MIN_SIZE_LOG = 4;

    private static final int MIN_SIZE = (1 << MIN_SIZE_LOG); // Must be power of 2

    // --- Sentinels -------------------------
    // No-Match-Old - putIfMatch does updates only if it matches the old value,
    // and NO_MATCH_OLD basically counts as a wildcard match.
    private static final long NO_MATCH_OLD = Long.MAX_VALUE - 1; // Sentinel
    // Match-Any-not-null - putIfMatch does updates only if it find a real old
    // value.
    public static final long MATCH_ANY = Long.MAX_VALUE - 2; // Sentinel
    // This K/V pair has been deleted (but the Key slot is forever claimed).
    // The same Key can be reinserted with a new value later.
    private static final long TOMBSTONE = Long.MAX_VALUE;

    private static final long TOMBPRIME = -Long.MAX_VALUE;

    private static final long MAX_VALUE_LIMIT = Long.MAX_VALUE - 3;

    // --- dump ----------------------------------------------------------------

    private static void print_impl(final int i, final long K, final long V) {
        String p = V < 0 ? "prime_" : "";
        long V2 = -V;
        String VS = (V2 == TOMBSTONE) ? "tombstone" : String.valueOf(V2);
        System.out.println("[" + i + "]=(" + K + "," + p + VS + ")");
    }


    private static void print2_impl(final int i, final long K, final long V) {
        if (Math.abs(V) != TOMBSTONE)
            print_impl(i, K, V);
    }

    // Count of reprobes
    private final LongAdder _reprobes = new LongAdder();

    /**
     * Get and clear the current count of reprobes.  Reprobes happen on key
     * collisions, and a high reprobe rate may indicate a poor hash function or
     * weaknesses in the table resizing function.
     *
     * @return the count of reprobes since the last call to {@link  #_reprobes}
     * or since the table was created.
     */
    @SuppressWarnings("unused")
    public long reprobes() {
        return _reprobes.sumThenReset();
    }


    // --- reprobe_limit -----------------------------------------------------
    // Heuristic to decide if we have reprobed toooo many times.  Running over
    // the reprobe limit on a 'lockForRead' call acts as a 'miss'; on a 'put' call it
    // can trigger a table resize.  Several places must have exact agreement on
    // what the reprobe_limit is, so we share it here.
    private static int reprobe_limit(int len) {
        return REPROBE_LIMIT + (len >> 4);
    }

    // --- NonBlockingHashMapLongLong ----------------------------------------------
    // Constructors

    NonBlockingHashMapLongLong() {
        this(MIN_SIZE, true);
    }

    /**
     * Create a new NonBlockingHashMapLongLong with initial room for the given
     * number of elements, thus avoiding internal resizing operations to reach
     * an appropriate size.  Large numbers here when used with a small count of
     * elements will sacrifice space for a small amount of time gained.  The
     * initial size will be rounded up internally to the next larger power of 2.
     */
    @SuppressWarnings("unused")
    public NonBlockingHashMapLongLong(final int initial_sz) {
        this(initial_sz, true);
    }

    /**
     * Create a new NonBlockingHashMapLongLong, setting the space-for-speed
     * tradeoff.  {@code true} optimizes for space and is the default.  {@code
     * false} optimizes for speed and doubles space costs for roughly a 10%
     * speed improvement.
     */
    @SuppressWarnings("unused")
    public NonBlockingHashMapLongLong(final boolean opt_for_space) {
        this(1, opt_for_space);
    }

    /**
     * Create a new NonBlockingHashMapLongLong, setting both the initial size and
     * the space-for-speed tradeoff.  {@code true} optimizes for space and is
     * the default.  {@code false} optimizes for speed and doubles space costs
     * for roughly a 10% speed improvement.
     */
    public NonBlockingHashMapLongLong(final int initial_sz, final boolean opt_for_space) {
        _opt_for_space = opt_for_space;
        initialize(initial_sz);
    }

    private void initialize(final int initial_sz) {
        int i;                      // Convert to next largest power-of-2
        //noinspection StatementWithEmptyBody
        for (i = MIN_SIZE_LOG; (1 << i) < initial_sz; i++) {/*empty*/}
        _chm = new CHM(this, new LongAdder(), i);
        _last_resize_milli = System.currentTimeMillis();
    }

    // --- wrappers ------------------------------------------------------------

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    public int size() {
        return _chm.size();
    }

    /**
     * Tests if the key in the table.
     *
     * @return <tt>true</tt> if the key is in the table
     */
    @SuppressWarnings("unused")
    public boolean containsKey(long key) {
        return get(key) >= 0;
    }

    /**
     * Maps the specified key to the specified value in the table.  The value
     * cannot be null.  <p> The value can be retrieved by calling {@link #get}
     * with a key that is equal to the original key.
     *
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>
     * @throws NullPointerException if the specified value is null
     */
    public long put(long key, long val) {
        return putIfMatch(key, val, NO_MATCH_OLD);
    }

    /**
     * Atomically, do a {@link #put} if-and-only-if the key is not mapped.
     * Useful to ensure that only a single mapping for the key exists, even if
     * many threads are trying to create the mapping in parallel.
     *
     * @return the previous value associated with the specified key,
     * or <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified is value is null
     */
    @SuppressWarnings("unused")
    public long putIfAbsent(long key, long val) {
        return putIfMatch(key, val, TOMBSTONE);
    }

    /**
     * Removes the key (and its corresponding value) from this map.
     * This method does nothing if the key is not in the map.
     *
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>
     */
    @SuppressWarnings("unused")
    public long remove(long key) {
        return putIfMatch(key, TOMBSTONE, NO_MATCH_OLD);
    }

    /**
     * Atomically do a {@link #remove(long)} if-and-only-if the key is mapped
     * to a value which is <code>equals</code> to the given value.
     *
     * @throws NullPointerException if the specified value is null
     */
    @SuppressWarnings("unused")
    public boolean remove(long key, long val) {
        return putIfMatch(key, TOMBSTONE, val) == val;
    }

    /**
     * Atomically do a <code>put(key,val)</code> if-and-only-if the key is
     * mapped to some value already.
     *
     * @throws NullPointerException if the specified value is null
     */
    public long replace(long key, long val) {
        return putIfMatch(key, val, MATCH_ANY);
    }

    /**
     * Atomically do a <code>put(key,newValue)</code> if-and-only-if the key is
     * mapped a value which is <code>equals</code> to <code>oldValue</code>.
     *
     * @throws NullPointerException if the specified value is null
     */
    public boolean replace(long key, long oldValue, long newValue) {
        return putIfMatch(key, newValue, oldValue) == oldValue;
    }

    private long putIfMatch(long key, long newVal, long oldVal) {
        assert oldVal >= 0;
        assert newVal >= 0;
        assert key >= 0;

        final long res = _chm.putIfMatch(key + 1, newVal <= MAX_VALUE_LIMIT ? newVal + 1 : newVal,
                oldVal <= MAX_VALUE_LIMIT ? oldVal + 1 : oldVal);

        assert res > 0;
        return res == TOMBSTONE ? -1 : res - 1;
    }

    /**
     * Removes all of the mappings from this map.
     */
    @SuppressWarnings("unused")
    public void clear() {         // Smack a new empty table down
        CHM newchm = new CHM(this, new LongAdder(), MIN_SIZE_LOG);
        //noinspection StatementWithEmptyBody
        while (!CHM_HANDLE.compareAndSet(this, _chm, newchm)) { /*Spin until the clear works*/}
    }

    // Non-atomic clear, preserving existing large arrays
    @SuppressWarnings("unused")
    public void clear(boolean large) {         // Smack a new empty table down
        _chm.clear();
    }

    // --- lockForRead -----------------------------------------------------------------

    /**
     * Returns the value to which the specified key is mapped, or {@code null}
     * if this map contains no mapping for the key.
     * <p>More formally, if this map contains a mapping from a key {@code k} to
     * a value {@code v} such that {@code key==k}, then this method
     * returns {@code v}; otherwise it returns {@code null}.  (There can be at
     * most one such mapping.)
     *
     * @throws NullPointerException if the specified key is null
     */
    // Never returns a Prime nor a Tombstone.
    public final long get(long key) {
        final long V = _chm.get_impl(key + 1);
        assert V != TOMBSTONE;

        return V >= 0 ? V - 1 : V;
    }


    // --- help_copy -----------------------------------------------------------
    // Help along an existing resize operation.  This is just a fast cut-out
    // wrapper, to encourage inlining for the fast no-copy-in-progress case.  We
    // always help the top-most table copy, even if there are nested table
    // copies in progress.
    private void help_copy() {
        // Read the top-level CHM only once.  We'll try to help this copy along,
        // even if it gets promoted out from under us (i.e., the copy completes
        // and another KVS becomes the top-level copy).
        CHM topchm = _chm;
        if (topchm._newchm == null) return; // No copy in-progress
        topchm.help_copy_impl();
    }

    // --- hash ----------------------------------------------------------------
    private static int hash(long h) {
        return (int) XxHash.hash(h);
    }


    // --- CHM -----------------------------------------------------------------
    // The control structure for the NonBlockingHashMapLongLong
    private static final class CHM implements Serializable {
        // Back-pointer to top-level structure
        final NonBlockingHashMapLongLong _nbhml;

        // Size in active K,V pairs
        private LongAdder _size;

        public int size() {
            return (int) _size.sum();
        }

        // ---
        // These next 2 fields are used in the resizing heuristics, to judge when
        // it is time to resize or copy the table.  Slots is a count of used-up
        // key slots, and when it nears a large fraction of the table we probably
        // end up reprobing too much.  Last-resize-milli is the time since the
        // last resize; if we are running back-to-back resizes without growing
        // (because there are only a few live keys but many slots full of dead
        // keys) then we need a larger table to cut down on the churn.

        // Count of used slots, to tell when table is full of dead unusable slots
        private LongAdder _slots;

        // ---
        // New mappings, used during resizing.
        // The 'next' CHM - created during a resize operation.  This represents
        // the new table being copied from the old one.  It's the volatile
        // variable that is read as we cross from one table to the next, to lockForRead
        // the required memory orderings.  It monotonically transits from null to
        // set (once).
        volatile CHM _newchm;
        private static final AtomicReferenceFieldUpdater<CHM, CHM> _newchmUpdater =
                AtomicReferenceFieldUpdater.newUpdater(CHM.class, CHM.class, "_newchm");

        // Set the _newchm field if we can.  AtomicUpdaters do not fail spuriously.
        boolean CAS_newchm(CHM newchm) {
            return _newchmUpdater.compareAndSet(this, null, newchm);
        }

        // Sometimes many threads race to create a new very large table.  Only 1
        // wins the race, but the losers all allocate a junk large table with
        // hefty allocation costs.  Attempt to control the overkill here by
        // throttling attempts to create a new table.  I cannot really block here
        // (lest I lose the non-blocking property) but late-arriving threads can
        // give the initial resizing thread a little time to allocate the initial
        // new table.  The Right Long Term Fix here is to use array-lets and
        // incrementally create the new very large array.  In C I'd make the array
        // with malloc (which would mmap under the hood) which would only eat
        // virtual-address and not real memory - and after Somebody wins then we
        // could in parallel initialize the array.  Java does not allow
        // un-initialized array creation (especially of ref arrays!).
        volatile long _resizers;    // count of threads attempting an initial resize
        private static final AtomicLongFieldUpdater<CHM> _resizerUpdater =
                AtomicLongFieldUpdater.newUpdater(CHM.class, "_resizers");

        private static final VarHandle AA
                = MethodHandles.arrayElementVarHandle(long[].class);


        // --- key,val -------------------------------------------------------------
        // Access K,V for a given idx
        private boolean casKey(int idx, long key) {
            return AA.compareAndSet(_keys, idx, (long) 0, key);
        }

        private boolean casVal(int idx, Object old, Object val) {
            return AA.compareAndSet(_vals, idx, old, val);
        }

        final long[] _keys;
        final long[] _vals;

        // Simple constructor
        CHM(final NonBlockingHashMapLongLong nbhml, LongAdder size, final int logsize) {
            _nbhml = nbhml;
            _size = size;
            _slots = new LongAdder();
            _keys = new long[1 << logsize];
            _vals = new long[1 << logsize];
        }

        // Non-atomic clear
        void clear() {
            _size = new LongAdder();
            _slots = new LongAdder();
            Arrays.fill(_keys, 0);
            Arrays.fill(_vals, 0);
        }

        // --- print innards
        @SuppressWarnings("unused")
        private void print() {
            for (int i = 0; i < _keys.length; i++) {
                long K = _keys[i];
                if (K != 0)
                    print_impl(i, K, _vals[i]);
            }
            CHM newchm = _newchm;     // New table, if any
            if (newchm != null) {
                System.out.println("----");
                newchm.print();
            }
        }

        // --- print only the live objects
        @SuppressWarnings("unused")
        private void print2() {
            for (int i = 0; i < _keys.length; i++) {
                long K = _keys[i];
                if (K != 0)       // key is sane
                    print2_impl(i, K, _vals[i]);
            }
            CHM newchm = _newchm;     // New table, if any
            if (newchm != null) {
                System.out.println("----");
                newchm.print2();
            }
        }

        // --- get_impl ----------------------------------------------------------
        // Never returns a Prime nor a Tombstone.
        private long get_impl(final long key) {
            final int hash = hash(key);
            final int len = _keys.length;
            int idx = (hash & (len - 1)); // First key hash

            // Main spin/reprobe loop, looking for a Key hit
            int reprobe_cnt = 0;
            var probeLimit = reprobe_limit(len);

            while (true) {
                final long K = _keys[idx]; // Get key   before volatile read, could be NO_KEY
                final long V = _vals[idx]; // Get value before volatile read, could be null or Tombstone or Prime
                if (K == 0) return -1; // A clear miss

                // Key-compare
                if (key == K) {
                    // Key hit!  Check for no table-copy-in-progress
                    if (V >= 0) { // No copy?
                        if (V == TOMBSTONE) return -1;
                        // We need a volatile-read between reading a newly inserted Value
                        // and returning the Value (so the user might end up reading the
                        // stale Value contents).
                        VarHandle.loadLoadFence();
                        return V;
                    }
                    // Key hit - but slot is (possibly partially) copied to the new table.
                    // Finish the copy & retry in the new table.
                    return copy_slot_and_check(idx, key).get_impl(key); // Retry in the new table
                }
                // lockForRead and put must have the same key lookup logic!  But only 'put'
                // needs to force a table-resize for a too-long key-reprobe sequence.
                // Check for too-many-reprobes on lockForRead.
                reprobe_cnt++;
                if (reprobe_cnt >= probeLimit) // too many probes
                    return _newchm == null // Table copy in progress?
                            ? 0               // Nope!  A clear miss
                            : copy_slot_and_check(idx, key).get_impl(key); // Retry in the new table

                idx = (idx + 1) & (len - 1);    // Reprobe by 1!  (could now prefetch)
            }
        }

        // --- putIfMatch ---------------------------------------------------------
        // Put, Remove, PutIfAbsent, etc.  Return the old value.  If the returned
        // value is equal to expVal (or expVal is NO_MATCH_OLD) then the put can
        // be assumed to work (although might have been immediately overwritten).
        // Only the path through copy_slot passes in an expected value of 0,
        // and putIfMatch only returns a 0 if passed in an expected 0.
        private long putIfMatch(final long key, final long putval, final long expVal) {
            final int hash = hash(key);
            assert putval >= 0;
            assert expVal >= 0;
            final int len = _keys.length;
            int idx = (hash & (len - 1)); // The first key

            // ---
            // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
            int reprobe_cnt = 0;
            long K;
            long V;
            while (true) {           // Spin till we lockForRead a Key slot
                V = _vals[idx];         // Get old value
                K = _keys[idx];         // Get current key
                if (K == 0) {     // Slot is free?
                    // Found an empty Key slot - which means this Key has never been in
                    // this table.  No need to put a Tombstone - the Key is not here!
                    if (putval == TOMBSTONE) return TOMBSTONE; // Not-now & never-been in this table
                    if (expVal == MATCH_ANY) return TOMBSTONE; // Will not match, even after K inserts
                    // Claim the zero key-slot
                    if (casKey(idx, key)) { // Claim slot for Key
                        _slots.add(1);      // Raise key-slots-used count
                        break;              // Got it!
                    }
                    // CAS to claim the key-slot failed.
                    //
                    // This re-read of the Key points out an annoying short-coming of Java
                    // CAS.  Most hardware CAS's report back the existing value - so that
                    // if you fail you have a *witness* - the value which caused the CAS
                    // to fail.  The Java API turns this into a boolean destroying the
                    // witness.  Re-reading does not recover the witness because another
                    // thread can write over the memory after the CAS.  Hence we can be in
                    // the unfortunate situation of having a CAS fail *for cause* but
                    // having that cause removed by a later store.  This turns a
                    // non-spurious-failure CAS (such as Azul has) into one that can
                    // apparently spuriously fail - and we avoid apparent spurious failure
                    // by not allowing Keys to ever change.
                    K = _keys[idx];       // CAS failed, lockForRead updated value
                    assert K != 0;  // If keys[idx] is NO_KEY, CAS shoulda worked
                }
                // Key slot was not null, there exists a Key here
                if (K == key)
                    break;                // Got it!

                // lockForRead and put must have the same key lookup logic!  Lest 'lockForRead' give
                // up looking too soon.
                //topmap._reprobes.add(1);
                if (++reprobe_cnt >= reprobe_limit(len)) {
                    // We simply must have a new table to do a 'put'.  At this point a
                    // 'lockForRead' will also go to the new table (if any).  We do not need
                    // to claim a key slot (indeed, we cannot find a free one to claim!).
                    final CHM newchm = resize();
                    if (expVal != 0) _nbhml.help_copy(); // help along an existing copy
                    return newchm.putIfMatch(key, putval, expVal);
                }

                idx = (idx + 1) & (len - 1); // Reprobe!
            } // End of spinning till we lockForRead a Key slot

            while (true) {              // Spin till we insert a value
                // ---
                // Found the proper Key slot, now update the matching Value slot.  We
                // never put a null, so Value slots monotonically move from null to
                // not-null (deleted Values use Tombstone).  Thus if 'V' is null we
                // fail this fast cutout and fall into the check for table-full.
                if (putval == V) return V; // Fast cutout for no-change

                // See if we want to move to a new table (to avoid high average re-probe
                // counts).  We only check on the initial set of a Value from null to
                // not-null (i.e., once per key-insert).
                if ((V == 0 && tableFull(reprobe_cnt, len)) ||
                        // Or we found a Prime: resize is already in progress.  The resize
                        // call below will do a CAS on _newchm forcing the read.
                        V < 0) {
                    resize();               // Force the new table copy to start
                    return copy_slot_and_check(idx, expVal).putIfMatch(key, putval, expVal);
                }

                // ---
                // We are finally prepared to update the existing table
                //assert !(V instanceof Prime); // always true, so IDE warnings if uncommented

                // Must match old, and we do not?  Then bail out now.  Note that either V
                // or expVal might be TOMBSTONE.  Also V can be null, if we've never
                // inserted a value before.  expVal can be null if we are called from
                // copy_slot.

                // Do we care about expected-Value at all?
                // No instant match already?
                // Match on null/TOMBSTONE combo
                if (expVal != NO_MATCH_OLD && V != expVal &&
                        (expVal != MATCH_ANY || V == TOMBSTONE || V == 0) &&
                        !(V == 0 && expVal == TOMBSTONE))
                    return (V == 0) ? TOMBSTONE : V;         // Do not update!

                // Actually change the Value in the Key,Value pair
                if (casVal(idx, V, putval)) break;

                // CAS failed
                // Because we have no witness, we do not know why it failed.
                // Indeed, by the time we look again the value under test might have flipped
                // a thousand times and now be the expected value (despite the CAS failing).
                // Check for the never-succeed condition of a Prime value and jump to any
                // nested table, or else just re-run.

                // We would not need this load at all if CAS returned the value on which
                // the CAS failed (AKA witness). The new CAS semantics are supported via
                // VarHandle in JDK9.
                V = _vals[idx];         // Get new value

                // If a Prime'd value got installed, we need to re-run the put on the
                // new table.  Otherwise we lost the CAS to another racing put.
                // Simply retry from the start.
                if (V < 0) {
                    return copy_slot_and_check(idx, expVal).putIfMatch(key, putval, expVal);
                }

                // Simply retry from the start.
                // NOTE: need the fence, since otherwise '_vals[idx]' load could be hoisted
                // out of loop.
                VarHandle.loadLoadFence();
            }

            // CAS succeeded - we did the update!
            // Both normal put's and table-copy calls putIfMatch, but table-copy
            // does not (effectively) increase the number of live k/v pairs.
            if (expVal != 0) {
                // Adjust sizes - a striped counter
                if ((V == 0 || V == TOMBSTONE) && putval != TOMBSTONE) _size.add(1);
                if (!(V == 0 || V == TOMBSTONE) && putval == TOMBSTONE) _size.add(-1);
            }

            // We won; we know the update happened as expected.
            return (V == 0 && expVal != 0) ? TOMBSTONE : V;
        }

        // --- tableFull ---------------------------------------------------------
        // Heuristic to decide if this table is too full, and we should start a
        // new table.  Note that if a 'lockForRead' call has reprobed too many times and
        // decided the table must be full, then always the estimate_sum must be
        // high and we must report the table is full.  If we do not, then we might
        // end up deciding that the table is not full and inserting into the
        // current table, while a 'lockForRead' has decided the same key cannot be in this
        // table because of too many reprobes.  The invariant is:
        //   slots.estimate_sum >= max_reprobe_cnt >= reprobe_limit(len)
        private boolean tableFull(int reprobe_cnt, int len) {
            return
                    // Do the cheap check first: we allow some number of reprobes always
                    reprobe_cnt >= REPROBE_LIMIT &&
                            (reprobe_cnt >= reprobe_limit(len) ||
                                    // More expensive check: see if the table is > 1/2 full.
                                    _slots.sum() >= (len >> 1));
        }

        // --- resize ------------------------------------------------------------
        // Resizing after too many probes.  "How Big???" heuristics are here.
        // Callers will (not this routine) will 'help_copy' any in-progress copy.
        // Since this routine has a fast cutout for copy-already-started, callers
        // MUST 'help_copy' lest we have a path which forever runs through
        // 'resize' only to discover a copy-in-progress which never progresses.
        private CHM resize() {
            // Check for resize already in progress, probably triggered by another thread
            CHM newchm = _newchm;     // VOLATILE READ
            if (newchm != null)      // See if resize is already in progress
                return newchm;          // Use the new table already

            // No copy in-progress, so start one.  First up: compute new table size.
            int oldlen = _keys.length; // Old count of K,V pairs allowed
            int sz = size();          // Get current table count of active K,V pairs
            int newsz = sz;           // First size estimate

            // Heuristic to determine new size.  We expect plenty of dead-slots-with-keys
            // and we need some decent padding to avoid endless reprobing.
            if (_nbhml._opt_for_space) {
                // This heuristic leads to a much denser table with a higher reprobe rate
                if (sz >= (oldlen >> 1)) // If we are >50% full of keys then...
                    newsz = oldlen << 1;    // Double size
            } else {
                if (sz >= (oldlen >> 2)) { // If we are >25% full of keys then...
                    newsz = oldlen << 1;      // Double size
                    if (sz >= (oldlen >> 1)) // If we are >50% full of keys then...
                        newsz = oldlen << 2;    // Double double size
                }
            }

            // Last (re)size operation was very recent?  Then double again
            // despite having few live keys; slows down resize operations
            // for tables subject to a high key churn rate - but do not
            // forever grow the table.  If there is a high key churn rate
            // the table needs a steady state of rare same-size resize
            // operations to clean out the dead keys.
            long tm = System.currentTimeMillis();
            if (newsz <= oldlen &&    // New table would shrink or hold steady?
                    tm <= _nbhml._last_resize_milli + 10000)  // Recent resize (less than 10 sec ago)
                newsz = oldlen << 1;      // Double the existing size

            // Do not shrink, ever.  If we hit this size once, assume we
            // will again.
            if (newsz < oldlen) newsz = oldlen;

            // Convert to power-of-2
            int log2;
            //noinspection StatementWithEmptyBody
            for (log2 = MIN_SIZE_LOG; (1 << log2) < newsz; log2++) ; // Compute log2 of size
            long len = ((1L << log2) << 1) + 2;
            // prevent integer overflow - limit of 2^31 elements in a Java array
            // so here, 2^30 + 2 is the largest number of elements in the hash table
            if ((int) len != len) {
                log2 = 30;
                len = (1L << log2) + 2;
                if (sz > ((len >> 2) + (len >> 1))) throw new RuntimeException("Table is full.");
            }

            // Now limit the number of threads actually allocating memory to a
            // handful - lets we have 750 threads all trying to allocate a giant
            // resized array.
            long r = _resizers;
            while (!_resizerUpdater.compareAndSet(this, r, r + 1))
                r = _resizers;
            // Size calculation: 2 words (K+V) per table entry, plus a handful.  We
            // guess at 64-bit pointers; 32-bit pointers screws up the size calc by
            // 2x but does not screw up the heuristic very much.
            long megs = ((((1L << log2) << 1) + 8) << 3/*word to bytes*/) >> 20/*megs*/;
            if (r >= 2 && megs > 0) { // Already 2 guys trying; wait and see
                newchm = _newchm;        // Between dorking around, another thread did it
                if (newchm != null)     // See if resize is already in progress
                    return newchm;         // Use the new table already
                // We could use a wait with timeout, so we'll wakeup as soon as the new table
                // is ready, or after the timeout in any case.
                //synchronized( this ) { wait(8*megs); }         // Timeout - we always wakeup
                // For now, sleep a tad and see if the 2 guys already trying to make
                // the table actually lockForRead around to making it happen.
                try {
                    Thread.sleep(megs);
                } catch (Exception e) { /*empty*/}
            }
            // Last check, since the 'new' below is expensive and there is a chance
            // that another thread slipped in a new thread while we ran the heuristic.
            newchm = _newchm;
            if (newchm != null)      // See if resize is already in progress
                return newchm;          // Use the new table already

            // New CHM - actually allocate the big arrays
            newchm = new CHM(_nbhml, _size, log2);

            // Another check after the slow allocation
            if (_newchm != null)     // See if resize is already in progress
                return _newchm;         // Use the new table already

            // The new table must be CAS'd in so only 1 winner amongst duplicate
            // racing resizing threads.  Extra CHM's will be GC'd.
            if (!CAS_newchm(newchm)) {
                newchm = _newchm;       // CAS failed? Reread new table
            }


            return newchm;
        }

        // The next part of the table to copy.  It monotonically transits from zero
        // to _keys.length.  Visitors to the table can claim 'work chunks' by
        // CAS'ing this field up, then copying the indicated indices from the old
        // table to the new table.  Workers are not required to finish any chunk;
        // the counter simply wraps and work is copied duplicately until somebody
        // somewhere completes the count.
        volatile long _copyIdx = 0;
        static private final AtomicLongFieldUpdater<CHM> _copyIdxUpdater =
                AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyIdx");

        // Work-done reporting.  Used to efficiently signal when we can move to
        // the new table.  From 0 to len(oldkvs) refers to copying from the old
        // table to the new.
        volatile long _copyDone = 0;
        static private final AtomicLongFieldUpdater<CHM> _copyDoneUpdater =
                AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyDone");

        // --- help_copy_impl ----------------------------------------------------
        // Help along an existing resize operation.  We hope its the top-level
        // copy (it was when we started) but this CHM might have been promoted out
        // of the top position.
        private void help_copy_impl() {
            final CHM newchm = _newchm;
            assert newchm != null;    // Already checked by caller
            int oldlen = _keys.length; // Total amount to copy
            final int MIN_COPY_WORK = Math.min(oldlen, 1024); // Limit per-thread work

            // ---
            int panic_start = -1;
            int copyidx = -9999;            // Fool javac to think it's initialized
            while (_copyDone < oldlen) { // Still needing to copy?
                // Carve out a chunk of work.  The counter wraps around so every
                // thread eventually tries to copy every slot repeatedly.

                // We "panic" if we have tried TWICE to copy every slot - and it still
                // has not happened.  i.e., twice some thread somewhere claimed they
                // would copy 'slot X' (by bumping _copyIdx) but they never claimed to
                // have finished (by bumping _copyDone).  Our choices become limited:
                // we can wait for the work-claimers to finish (and become a blocking
                // algorithm) or do the copy work ourselves.  Tiny tables with huge
                // thread counts trying to copy the table often 'panic'.
                if (panic_start == -1) { // No panic?
                    copyidx = (int) _copyIdx;
                    while (copyidx < (oldlen << 1) && // 'panic' check
                            !_copyIdxUpdater.compareAndSet(this, copyidx, copyidx + MIN_COPY_WORK))
                        copyidx = (int) _copyIdx;     // Re-read
                    if (!(copyidx < (oldlen << 1))) // Panic!
                        panic_start = copyidx;       // Record where we started to panic-copy
                }

                // We now know what to copy.  Try to copy.
                int workdone = 0;
                for (int i = 0; i < MIN_COPY_WORK; i++)
                    if (copy_slot((copyidx + i) & (oldlen - 1))) // Made an oldtable slot go dead?
                        workdone++;         // Yes!
                if (workdone > 0)      // Report work-done occasionally
                    copy_check_and_promote(workdone);// See if we can promote

                copyidx += MIN_COPY_WORK;
                // Uncomment these next 2 lines to turn on incremental table-copy.
                // Otherwise this thread continues to copy until it is all done.
                if (panic_start == -1) // No panic?
                    return;               // Then done copying after doing MIN_COPY_WORK
            }
            // Extra promotion check, in case another thread finished all copying
            // then got stalled before promoting.
            copy_check_and_promote(0); // See if we can promote
        }


        // --- copy_slot_and_check -----------------------------------------------
        // Copy slot 'idx' from the old table to the new table.  If this thread
        // confirmed the copy, update the counters and check for promotion.
        //
        // Returns the result of reading the volatile _newchm, mostly as a
        // convenience to callers.  We come here with 1-shot copy requests
        // typically because the caller has found a Prime, and has not yet read
        // the _newchm volatile - which must have changed from null-to-not-null
        // before any Prime appears.  So the caller needs to read the _newchm
        // field to retry his operation in the new table, but probably has not
        // read it yet.
        private CHM copy_slot_and_check(int idx, long should_help) {
            // We're only here because the caller saw a Prime, which implies a
            // table-copy is in progress.
            assert _newchm != null;
            if (copy_slot(idx))      // Copy the desired slot
                copy_check_and_promote(1); // Record the slot copied
            // Generically help along any copy (except if called recursively from a helper)
            if (should_help != 0) _nbhml.help_copy();
            return _newchm;
        }

        // --- copy_check_and_promote --------------------------------------------
        private void copy_check_and_promote(int workdone) {
            int oldlen = _keys.length;
            // We made a slot unusable and so did some of the needed copy work
            long copyDone = _copyDone;
            long nowDone = copyDone + workdone;
            assert nowDone <= oldlen;
            if (workdone > 0) {
                while (!_copyDoneUpdater.compareAndSet(this, copyDone, nowDone)) {
                    copyDone = _copyDone;   // Reload, retry
                    nowDone = copyDone + workdone;
                    assert nowDone <= oldlen;
                }
            }

            // Check for copy being ALL done, and promote.  Note that we might have
            // nested in-progress copies and manage to finish a nested copy before
            // finishing the top-level copy.  We only promote top-level copies.
            if (nowDone == oldlen &&   // Ready to promote this table?
                    _nbhml._chm == this && // Looking at the top-level table?
                    // Attempt to promote
                    CHM_HANDLE.compareAndSet(_nbhml, this, _newchm)) {
                _nbhml._last_resize_milli = System.currentTimeMillis();  // Record resize time for next check
            }
        }

        // --- copy_slot ---------------------------------------------------------
        // Copy one K/V pair from oldkvs[i] to newkvs.  Returns true if we can
        // confirm that we set an old-table slot to TOMBPRIME, and only returns after
        // updating the new table.  We need an accurate confirmed-copy count so
        // that we know when we can promote (if we promote the new table too soon,
        // other threads may 'miss' on values not-yet-copied from the old table).
        // We don't allow any direct updates on the new table, unless they first
        // happened to the old table - so that any transition in the new table from
        // null to not-null must have been from a copy_slot (or other old-table
        // overwrite) and not from a thread directly writing in the new table.
        private boolean copy_slot(int idx) {
            // Blindly set the key slot from NO_KEY to some key which hashes here,
            // to eagerly stop fresh put's from inserting new values in the old
            // table when the old table is mid-resize.  We don't need to act on the
            // results here, because our correctness stems from box'ing the Value
            // field.  Slamming the Key field is a minor speed optimization.
            long key;
            while ((key = _keys[idx]) == 0)
                casKey(idx, (idx + _keys.length)/*a non-zero key which hashes here*/);

            // ---
            // Prevent new values from appearing in the old table.
            // Box what we see in the old table, to prevent further updates.
            long oldval = _vals[idx]; // Read OLD table
            while (oldval >= 0) {
                final long box = (oldval == 0 || oldval == TOMBSTONE) ? TOMBPRIME : -oldval;
                if (casVal(idx, oldval, box)) { // CAS down a box'd version of oldval
                    // If we made the Value slot hold a TOMBPRIME, then we both
                    // prevented further updates here but also the (absent) oldval is
                    // vaccuously available in the new table.  We return with true here:
                    // any thread looking for a value for this key can correctly go
                    // straight to the new table and skip looking in the old table.
                    if (box == TOMBPRIME)
                        return true;
                    // Otherwise we boxed something, but it still needs to be
                    // copied into the new table.
                    oldval = box;         // Record updated oldval
                    break;                // Break loop; oldval is now boxed by us
                }
                oldval = _vals[idx];    // Else try, try again
            }
            if (oldval == TOMBPRIME) return false; // Copy already complete here!

            // ---
            // Copy the value into the new table, but only if we overwrite a null.
            // If another value is already in the new table, then somebody else
            // wrote something there and that write is happens-after any value that
            // appears in the old table.
            long old_unboxed = -oldval;
            boolean copied_into_new = (_newchm.putIfMatch(key, old_unboxed, 0) == 0);

            // ---
            // Finally, now that any old value is exposed in the new table, we can
            // forever hide the old-table value by slapping a TOMBPRIME down.  This
            // will stop other threads from uselessly attempting to copy this slot
            // (i.e., it's a speed optimization not a correctness issue).
            while (oldval != TOMBPRIME && !casVal(idx, oldval, TOMBPRIME))
                oldval = _vals[idx];

            return copied_into_new;
        } // end copy_slot
    } // End of CHM
}
