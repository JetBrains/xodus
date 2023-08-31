package jetbrains.exodus.diskann.diskcache;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.sun.nio.file.ExtendedOpenOption;
import jetbrains.exodus.diskann.util.collections.BlockingLongArrayQueue;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jctools.queues.MpscGrowableArrayQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.WeakReference;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Locale.US;
import static jetbrains.exodus.diskann.diskcache.Node.PROBATION;
import static jetbrains.exodus.diskann.diskcache.Node.PROTECTED;
import static jetbrains.exodus.diskann.diskcache.Node.WINDOW;

public final class DiskCache extends BLCHeader.DrainStatusRef implements AutoCloseable {
    /*
     * This class performs a best-effort bounding of a ConcurrentHashMap using a page-replacement
     * algorithm to determine which entries to evict when the capacity is exceeded.
     *
     * Concurrency:
     * ------------
     * The page replacement algorithms are kept eventually consistent with the map. An update to the
     * map and recording of reads may not be immediately reflected in the policy's data structures.
     * These structures are guarded by a lock, and operations are applied in batches to avoid lock
     * contention. The penalty of applying the batches is spread across threads, so that the amortized
     * cost is slightly higher than performing just the ConcurrentHashMap operation [1].
     *
     * A memento of the reads and writes that were performed on the map is recorded in buffers. These
     * buffers are drained at the first opportunity after a write or when a read buffer is full. The
     * reads are offered to a buffer that will reject additions if contended on or if it is full. Due
     * to the concurrent nature of the read and write operations, a strict policy ordering is not
     * possible, but it may be observably strict when single-threaded. The buffers are drained
     * asynchronously to minimize the request latency and uses a state machine to determine when to
     * schedule this work on an executor.
     *
     * Due to a lack of a strict ordering guarantee, a task can be executed out-of-order, such as a
     * removal followed by its addition. The state of the entry is encoded using the key field to
     * avoid additional memory usage. An entry is "alive" if it is in both the hash table and the page
     * replacement policy. It is "retired" if it is not in the hash table and is pending removal from
     * the page replacement policy. Finally, an entry transitions to the "dead" state when it is
     * neither in the hash table nor the page replacement policy. Both the retired and dead states are
     * represented by a sentinel key that should not be used for map operations.
     *
     * Eviction:
     * ---------
     * Maximum size is implemented using the Window TinyLfu policy [2] due to its high hit rate, O(1)
     * time complexity, and small footprint. A new entry starts in the admission window and remains
     * there as long as it has high temporal locality (recency). Eventually an entry will slip from
     * the window into the main space. If the main space is already full, then a historic frequency
     * filter determines whether to evict the newly admitted entry or the victim entry chosen by the
     * eviction policy. This process ensures that the entries in the window were very recently used,
     * while entries in the main space are accessed very frequently and remain moderately recent. The
     * windowing allows the policy to have a high hit rate when entries exhibit a bursty access
     * pattern, while the filter ensures that popular items are retained. The admission window uses
     * LRU and the main space uses Segmented LRU.
     *
     * The optimal size of the window vs. main spaces is workload dependent [3]. A large admission
     * window is favored by recency-biased workloads, while a small one favors frequency-biased
     * workloads. When the window is too small, then recent arrivals are prematurely evicted, but when
     * it is too large, then they pollute the cache and force the eviction of more popular entries.
     * The optimal configuration is dynamically determined by using hill climbing to walk the hit rate
     * curve. This is achieved by sampling the hit rate and adjusting the window size in the direction
     * that is improving (making positive or negative steps). At each interval, the step size is
     * decreased until the hit rate climber converges at the optimal setting. The process is restarted
     * when the hit rate changes over a threshold, indicating that the workload altered, and a new
     * setting may be required.
     *
     * The historic usage is retained in a compact popularity sketch, which uses hashing to
     * probabilistically estimate an item's frequency. This exposes a flaw where an adversary could
     * use hash flooding [4] to artificially raise the frequency of the main space's victim and cause
     * all candidates to be rejected. In the worst case, by exploiting hash collisions, an attacker
     * could cause the cache to never hit and hold only worthless items, resulting in a
     * denial-of-service attack against the underlying resource. This is mitigated by introducing
     * jitter, allowing candidates that are at least moderately popular to have a small, random chance
     * of being admitted. This causes the victim to be evicted, but in a way that marginally impacts
     * the hit rate.
     *
     * [1] BP-Wrapper: A Framework Making Any Replacement Algorithms (Almost) Lock Contention Free
     * http://web.cse.ohio-state.edu/hpcs/WWW/HTML/publications/papers/TR-09-1.pdf
     * [2] TinyLFU: A Highly Efficient Cache Admission Policy
     * https://dl.acm.org/citation.cfm?id=3149371
     * [3] Adaptive Software Cache Management
     * https://dl.acm.org/citation.cfm?id=3274816
     * [4] Denial of Service via Algorithmic Complexity Attack
     * https://www.usenix.org/legacy/events/sec03/tech/full_papers/crosby/crosby.pdf
     * [5] Hashed and Hierarchical Timing Wheels
     * http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf
     */

    private static final Logger logger = LoggerFactory.getLogger(DiskCache.class);

    private static final int NCPU = Runtime.getRuntime().availableProcessors();
    /** The initial capacity of the write buffer. */
    private static final int ADD_BUFFER_MIN = 4;
    /** The maximum capacity of the write buffer. */
    private static final int ADD_BUFFER_MAX = 128 * Integer.highestOneBit(NCPU - 1) << 1;
    /** The number of attempts to insert into the write buffer before yielding. */
    private static final int ADD_BUFFER_RETRIES = 100;
    /** The initial percent of the maximum weighted capacity dedicated to the main space. */
    private static final double PERCENT_MAIN = 0.99d;
    /** The percent of the maximum weighted capacity dedicated to the main's protected space. */
    private static final double PERCENT_MAIN_PROTECTED = 0.80d;
    /** The difference in hit rates that restarts the climber. */
    private static final double HILL_CLIMBER_RESTART_THRESHOLD = 0.05d;
    /** The percent of the total size to adapt the window by. */
    private static final double HILL_CLIMBER_STEP_PERCENT = 0.0625d;
    /** The rate to decrease the step size to adapt by. */
    private static final double HILL_CLIMBER_STEP_DECAY_RATE = 0.98d;
    /** The minimum popularity for allowing randomized admission. */
    private static final int ADMIT_HASHDOS_THRESHOLD = 6;
    /** The maximum number of entries that can be transferred between queues. */
    private static final int QUEUE_TRANSFER_THRESHOLD = 1_000;

    private static final long WARN_AFTER_LOCK_WAIT_NANOS = TimeUnit.SECONDS.toNanos(30);

    private static final Object futurePlaceHolder = new Object();

    private final int filePagesCount;

    private final ReentrantLock evictionLock;
    private final PerformCleanupTask drainBuffersTask;

    private final FrequencySketch frequencySketch;

    private final BoundedBuffer<Node> readBuffer;

    private final MpscGrowableArrayQueue<Node> addBuffer;

    private final AccessOrderDeque<Node> accessOrderWindowDeque;
    private final AccessOrderDeque<Node> accessOrderProtectedDeque;
    private final AccessOrderDeque<Node> accessOrderProbationDeque;

    private long mainProtectedSize;

    private long windowSize;

    private long currentCacheSize;

    private long windowMaximum;

    private long mainProtectedMaximum;

    private final long maximum;

    private int hitsInSample;
    private int missesInSample;

    private double stepSize;

    private final NonBlockingHashMapLong<Node> data;

    private double previousSampleHitRate;

    private long adjustment;

    private final BlockingLongArrayQueue freePagesQueue;

    private final int pageSize;

    public final MemorySegment pages;

    private final Arena arena;

    public final VarHandle pagesVersionHandle;

    private final int preLoadersCount;

    private final ExecutorService pagePreLoaders;

    private final FileChannel fileChannel;

    private final NonBlockingHashMapLong<Object> preloadingPages = new NonBlockingHashMapLong<>();

    private final long vectorRecordOffset;
    private final long edgesCountOffset;
    private final long edgesOffset;

    private final int verticesCountPerPage;

    private final int vertexRecordSize;

    private final LongAdder requestsCount = new LongAdder();

    private final LongAdder hitsCount = new LongAdder();

    public DiskCache(long cacheSize, int vectorDim, int maxConnectionsPerVertex,
                     final Path graphFile) throws IOException {

        var pagesStructure = calculatePagesStructure(cacheSize,
                vectorDim, maxConnectionsPerVertex, graphFile);

        logger.info("DiskCache initialization : file block size {}, page size {},  " +
                        "pages in cache {} ({} Mb), " +
                        "size of page prefetching thread pool {}, " +
                        "memory allocated for preloading of pages {} Mb," +
                        "total memory allocated for the cache including memory allocated for page preloading {} Mb," +
                        "vertex record size {}, " +
                        "vertices count per page {}",
                pagesStructure.pageStructure.blockSize,
                pagesStructure.pageStructure.pageSize,
                pagesStructure.cachePagesCount,
                (long) pagesStructure.cachePagesCount * pagesStructure.pageStructure.pageSize / 1024 / 1024,

                pagesStructure.preLoadersCount,
                (long) (pagesStructure.allocatedPagesCount - pagesStructure.cachePagesCount)
                        * pagesStructure.pageStructure.pageSize / 1024 / 1024,

                (long) pagesStructure.allocatedPagesCount * pagesStructure.pageStructure.pageSize / 1024 / 1024,

                pagesStructure.pageStructure.vertexRecordSize,
                pagesStructure.pageStructure.verticesCountPerPage);

        this.pageSize = pagesStructure.pageStructure.pageSize;
        this.vertexRecordSize = pagesStructure.pageStructure.vertexRecordSize;
        this.edgesCountOffset = pagesStructure.pageStructure.recordEdgesCountOffset;
        this.edgesOffset = pagesStructure.pageStructure.recordEdgesOffset;
        this.verticesCountPerPage = pagesStructure.pageStructure.verticesCountPerPage;
        this.vectorRecordOffset = pagesStructure.pageStructure.recordVectorsOffset;

        fileChannel = FileChannel.open(graphFile, StandardOpenOption.READ,
                ExtendedOpenOption.DIRECT);

        this.filePagesCount = (int) (fileChannel.size() / pageSize);

        int cachePagesCount = pagesStructure.cachePagesCount;

        this.evictionLock = new ReentrantLock();
        this.drainBuffersTask = new PerformCleanupTask(this);

        this.frequencySketch = new FrequencySketch(cachePagesCount);
        this.readBuffer = new BoundedBuffer<>();

        this.accessOrderWindowDeque = new AccessOrderDeque<>();
        this.accessOrderProtectedDeque = new AccessOrderDeque<>();
        this.accessOrderProbationDeque = new AccessOrderDeque<>();


        //we keep preLoadersCount pages free in memory to always have some pages to load new data
        long window = (long) cachePagesCount - (long) (PERCENT_MAIN * (long) cachePagesCount);
        long mainProtected = (long) (PERCENT_MAIN_PROTECTED * ((long) cachePagesCount - window));

        this.maximum = cachePagesCount;

        this.windowMaximum = window;
        this.mainProtectedMaximum = mainProtected;

        hitsInSample = 0;
        missesInSample = 0;

        stepSize = -HILL_CLIMBER_STEP_PERCENT * (long) cachePagesCount;


        addBuffer = new MpscGrowableArrayQueue<>(ADD_BUFFER_MIN, ADD_BUFFER_MAX);
        data = new NonBlockingHashMapLong<>(pagesStructure.allocatedPagesCount, false);
        freePagesQueue = new BlockingLongArrayQueue(ADD_BUFFER_MAX);

        arena = Arena.openShared();

        pages = arena.allocate((long) pageSize * pagesStructure.allocatedPagesCount, pageSize);

        var pagesLayout = pagesStructure.pagesLayout;

        pagesVersionHandle = pagesLayout.varHandle(MemoryLayout.PathElement.sequenceElement(),
                MemoryLayout.PathElement.groupElement("pageVersion"));


        for (int i = 0; i < pagesStructure.allocatedPagesCount; i++) {
            freePagesQueue.enqueue(i);
        }

        preLoadersCount = pagesStructure.preLoadersCount;

        pagePreLoaders = Executors.newFixedThreadPool(preLoadersCount, r -> {
            var thread = new Thread(r);
            thread.setName("DiskCache page preloader");
            thread.setDaemon(true);
            return thread;
        });
    }

    public int preLoadersCount() {
        return preLoadersCount;
    }


    public long vectorOffset(long inMemoryPageIndex, long vertexIndex) {
        var recordOffset = (vertexIndex % verticesCountPerPage) * vertexRecordSize + Long.BYTES;
        return inMemoryPageIndex * pageSize + recordOffset + vectorRecordOffset;
    }

    public int fetchEdges(long vertexIndex, int[] edges, long[] inMemoryPageIndexVersion) {
        var recordOffset = (vertexIndex % verticesCountPerPage) * vertexRecordSize + Long.BYTES;
        var edgeCountOffset = recordOffset + edgesCountOffset;
        var edgesOffset = recordOffset + this.edgesOffset;

        long inMemoryPageIndex;
        long pageVersion;

        int edgesCount;
        do {
            get(vertexIndex, inMemoryPageIndexVersion);
            inMemoryPageIndex = inMemoryPageIndexVersion[0];
            pageVersion = inMemoryPageIndexVersion[1];

            edgesCount = pages.get(ValueLayout.JAVA_INT, edgeCountOffset);
            MemorySegment.copy(pages, ValueLayout.JAVA_INT, inMemoryPageIndex * pageSize + edgesOffset, edges, 0, edgesCount);

        } while (!checkVersion(inMemoryPageIndex, pageVersion));

        return edgesCount;
    }

    public boolean checkVersion(long inMemoryPageIndex, long pageVersion) {
        var currentVersion = (long) pagesVersionHandle.get(pages, inMemoryPageIndex);
        return currentVersion == pageVersion;
    }

    public void get(long vertexIndex, @NotNull long[] inMemoryPageIndexAndVersion) {
        long pageIndex = vertexIndex / verticesCountPerPage;

        while (true) {
            var found = getPageIfPresent(pageIndex, inMemoryPageIndexAndVersion);
            if (found) {
                return;
            }

            var future = preloadingPages.putIfAbsent(pageIndex, futurePlaceHolder);

            if (future == null) {
                schedulePagePreLoading(pageIndex);
            } else if (future == futurePlaceHolder) {
                Thread.onSpinWait();
            } else {
                try {
                    //noinspection unchecked
                    ((Future<Void>) future).get();
                    preloadingPages.remove(pageIndex, future);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public long hits() {
        return 100 * hitsCount.sum() / requestsCount.sum();
    }

    private boolean getPageIfPresent(long pageIndex, @NotNull long[] inMemoryPageIndexAndVersion) {
        if (pageIndex >= filePagesCount) {
            throw new IllegalArgumentException();
        }

        requestsCount.increment();
        var node = data.get(pageIndex + 1);

        if (node == null) {
            if (drainStatusOpaque() == REQUIRED) {
                scheduleDrainBuffers();
            }

            return false;
        }

        long inMemoryPageIndex = node.getValue();
        long pageVersion = node.pageVersion;

        afterRead(node);

        inMemoryPageIndexAndVersion[0] = inMemoryPageIndex;
        inMemoryPageIndexAndVersion[1] = pageVersion;

        hitsCount.increment();

        return true;
    }

    private boolean containsPage(long pageIndex) {
        if (pageIndex >= filePagesCount) {
            throw new IllegalArgumentException();
        }

        var node = data.get(pageIndex + 1);

        if (node == null) {
            if (drainStatusOpaque() == REQUIRED) {
                scheduleDrainBuffers();
            }

            return false;
        }

        afterRead(node);

        return true;
    }


    public void preloadIfNeeded(long vertexIndex) {
        long pageIndex = vertexIndex / verticesCountPerPage;

        if (containsPage(pageIndex)) {
            return;
        }

        var future = preloadingPages.putIfAbsent(pageIndex, futurePlaceHolder);

        if (future == null) {
            schedulePagePreLoading(pageIndex);
        }
    }

    private void schedulePagePreLoading(long pageIndex) {
        var future = CompletableFuture.runAsync(() -> {
            try {
                long inMemoryPageIndex;
                while (true) {
                    inMemoryPageIndex = freePagesQueue.dequeue();

                    if (inMemoryPageIndex == -1) {
                        //we booked all pages needed for preloading
                        //we need to wait till cache will free them.
                        performCleanUp();
                        continue;
                    }

                    break;
                }

                var pageSegment = pages.asSlice(pageSize * inMemoryPageIndex, pageSize);
                var buffer = pageSegment.asByteBuffer();

                var position = pageIndex * pageSize;
                while (buffer.remaining() > 0) {
                    var r = fileChannel.read(buffer, position + buffer.position());

                    if (r == -1) {
                        throw new EOFException();
                    }
                }

                var loadedPageVersion = (long) pagesVersionHandle.getVolatile(pages, inMemoryPageIndex);
                pagesVersionHandle.setVolatile(pages, inMemoryPageIndex, loadedPageVersion + 1);
                add(pageIndex, inMemoryPageIndex, loadedPageVersion + 1);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }, pagePreLoaders);

        var updated = preloadingPages.replace(pageIndex, futurePlaceHolder, future);

        if (!updated) {
            logger.error("Concurrent preloading of page {} !!!", pageIndex);
            throw new IllegalStateException();
        }
    }

    /**
     * Adds a node to the policy and the data store. If an existing node is found, then its value is
     * updated if allowed.
     *
     * @param key key with which the specified value is to be associated
     * @param inMemoryPageIndex index of the page inside the cache memory.
     * @param pageVersion version of page during the caching of the page.
     */
    private void add(long key, long inMemoryPageIndex, long pageVersion) {
        Node node = null;
        while (true) {
            Node prior = data.get(key + 1);

            if (prior == null) {
                if (node == null) {
                    node = new Node(key, inMemoryPageIndex, pageVersion);
                }

                prior = data.putIfAbsent(key + 1, node);

                if (prior == null) {
                    afterAdd(node);
                    return;
                } else {
                    if (!prior.isAlive()) {
                        continue;
                    }

                    throw new IllegalStateException("Page " + inMemoryPageIndex + " version " + prior.pageVersion +
                            " is already present in the cache.");
                }
            } else {
                if (!prior.isAlive()) {
                    continue;
                }

                throw new IllegalStateException("Page " + inMemoryPageIndex + " version " + prior.pageVersion +
                        " is already present in the cache.");
            }
        }
    }

    /**
     * Performs the post-processing work required after a write.
     *
     * @param node node to be executed on {@link #onAddTask(Node)}
     */
    private void afterAdd(Node node) {
        for (int i = 0; i < ADD_BUFFER_RETRIES; i++) {
            if (addBuffer.offer(node)) {
                scheduleAfterWrite();
                return;
            }

            scheduleDrainBuffers();
            Thread.onSpinWait();
        }

        // In scenarios where the writing threads cannot make progress then they attempt to provide
        // assistance by performing the eviction work directly. This can resolve cases where the
        // maintenance task is scheduled but not running. That might occur due to all of the executor's
        // threads being busy (perhaps writing into this cache), the write rate greatly exceeds the
        // consuming rate, priority inversion, or if the executor silently discarded the maintenance
        // task. Unfortunately this cannot resolve when the eviction is blocked waiting on a long-
        // running computation due to an eviction listener, the victim is being computed on by a writer,
        // or the victim residing in the same hash bin as a computing entry. In those cases a warning is
        // logged to encourage the application to decouple these computations from the map operations.
        lock();
        try {
            maintenance(node);
        } catch (RuntimeException e) {
            logger.error("Exception thrown when performing the maintenance task", e);
        } finally {
            evictionLock.unlock();
        }
        rescheduleCleanUpIfIncomplete();
    }

    /**
     * Conditionally schedules the asynchronous maintenance task after a write operation. If the
     * task status was IDLE or REQUIRED then the maintenance task is scheduled immediately. If it
     * is already processing then it is set to transition to REQUIRED upon completion so that a new
     * execution is triggered by the next operation.
     */
    private void scheduleAfterWrite() {
        int drainStatus = drainStatusOpaque();
        for (; ; ) {
            switch (drainStatus) {
                case IDLE -> {
                    casDrainStatus(IDLE, REQUIRED);
                    scheduleDrainBuffers();
                    return;
                }
                case REQUIRED -> {
                    scheduleDrainBuffers();
                    return;
                }
                case PROCESSING_TO_IDLE -> {
                    if (casDrainStatus(PROCESSING_TO_IDLE, PROCESSING_TO_REQUIRED)) {
                        return;
                    }
                    drainStatus = drainStatusAcquire();
                }
                case PROCESSING_TO_REQUIRED -> {
                    return;
                }
                default -> throw new IllegalStateException("Invalid drain status: " + drainStatus);
            }
        }
    }


    /** Acquires the eviction lock. */
    private void lock() {
        long remainingNanos = WARN_AFTER_LOCK_WAIT_NANOS;
        long end = System.nanoTime() + remainingNanos;
        boolean interrupted = false;
        try {
            for (; ; ) {
                try {
                    if (evictionLock.tryLock(remainingNanos, TimeUnit.NANOSECONDS)) {
                        return;
                    }
                    logger.warn("The cache is experiencing excessive wait times for acquiring "
                            + "the eviction lock. This may indicate that a long-running computation has halted "
                            + "eviction when trying to remove the victim entry. Consider using AsyncCache to "
                            + "decouple the computation from the map operation.", new TimeoutException());
                    evictionLock.lock();
                    return;
                } catch (InterruptedException e) {
                    remainingNanos = end - System.nanoTime();
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Performs the post-processing work required after a read.
     *
     * @param node the entry in the page replacement policy
     */
    private void afterRead(Node node) {
        boolean delayable = readBuffer.offer(node) != Buffer.FULL;

        if (shouldDrainBuffers(delayable)) {
            scheduleDrainBuffers();
        }
    }


    /**
     * Performs the maintenance work, blocking until the lock is acquired.
     */
    private void performCleanUp() {
        evictionLock.lock();
        try {
            maintenance(null);
        } finally {
            evictionLock.unlock();
        }
        rescheduleCleanUpIfIncomplete();
    }


    /**
     * If there remains pending operations that were not handled by the prior clean up then try to
     * schedule an asynchronous maintenance task. This may occur due to a concurrent write after the
     * maintenance work had started or if the amortized threshold of work per clean up was reached.
     */
    private void rescheduleCleanUpIfIncomplete() {
        if (drainStatusOpaque() != REQUIRED) {
            return;
        }

        scheduleDrainBuffers();
    }

    /**
     * Attempts to schedule an asynchronous task to apply the pending operations to the page
     * replacement policy. If the executor rejects the task then it is run directly.
     */
    private void scheduleDrainBuffers() {
        if (drainStatusOpaque() >= PROCESSING_TO_IDLE) {
            return;
        }
        if (evictionLock.tryLock()) {
            try {
                int drainStatus = drainStatusOpaque();
                if (drainStatus >= PROCESSING_TO_IDLE) {
                    return;
                }
                setDrainStatusRelease(PROCESSING_TO_IDLE);
                //noinspection resource
                ForkJoinPool.commonPool().execute((ForkJoinTask<?>) drainBuffersTask);
            } catch (Throwable t) {
                logger.warn("Exception thrown when submitting maintenance task", t);
                maintenance(/* ignored */ null);
            } finally {
                evictionLock.unlock();
            }
        }
    }


    /**
     * Performs the pending maintenance work and sets the state flags during processing to avoid
     * excess scheduling attempts. The read buffer, write buffer are drained,
     * followed by expiration, and size-based eviction.
     */
    @GuardedBy("evictionLock")
    private void maintenance(@Nullable Node node) {
        setDrainStatusRelease(PROCESSING_TO_IDLE);
        try {
            drainReadBuffer();

            drainWriteBuffer();
            if (node != null) {
                onAddTask(node);
            }

            evictEntries();

            climb();
        } finally {
            if ((drainStatusOpaque() != PROCESSING_TO_IDLE)
                    || !casDrainStatus(PROCESSING_TO_IDLE, IDLE)) {
                setDrainStatusOpaque(REQUIRED);
            }
        }
    }

    @GuardedBy("evictionLock")
    private void onAddTask(Node node) {
        currentCacheSize++;
        windowSize++;

        long key = node.getKey();
        if (key >= 0) {
            frequencySketch.increment(key);
        }

        missesInSample++;

        // ignore out-of-order write operations
        boolean isAlive;
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (node) {
            isAlive = node.isAlive();
        }
        if (isAlive) {
            accessOrderWindowDeque.offerLast(node);
        }
    }

    /** Adapts the eviction policy to towards the optimal recency / frequency configuration. */
    @GuardedBy("evictionLock")
    private void climb() {
        determineAdjustment();
        demoteFromMainProtected();

        long amount = adjustment;
        if (amount > 0) {
            increaseWindow();
        } else if (amount < 0) {
            decreaseWindow();
        }
    }

    /** Decreases the size of the admission window and increases the main's protected region. */
    @GuardedBy("evictionLock")
    private void decreaseWindow() {
        if (windowMaximum <= 1) {
            return;
        }

        long quota = Math.min(-adjustment, Math.max(0, windowMaximum - 1));
        mainProtectedMaximum += quota;
        windowMaximum -= quota;

        for (int i = 0; i < QUEUE_TRANSFER_THRESHOLD; i++) {
            Node candidate = accessOrderWindowDeque.peekFirst();
            if (candidate == null) {
                break;
            }


            quota--;
            windowSize--;

            accessOrderWindowDeque.remove(candidate);
            accessOrderProbationDeque.offerLast(candidate);
            candidate.makeMainProbation();
        }

        mainProtectedMaximum -= quota;
        windowMaximum += quota;
        adjustment = -quota;
    }


    /** Transfers the nodes from the protected to the probation region if it exceeds the maximum. */
    @GuardedBy("evictionLock")
    private void demoteFromMainProtected() {
        long mainProtectedMaximum = this.mainProtectedMaximum;
        long mainProtectedSize = this.mainProtectedSize;
        if (mainProtectedSize <= mainProtectedMaximum) {
            return;
        }

        for (int i = 0; i < QUEUE_TRANSFER_THRESHOLD; i++) {
            if (mainProtectedSize <= mainProtectedMaximum) {
                break;
            }

            Node demoted = accessOrderProtectedDeque.poll();
            if (demoted == null) {
                break;
            }
            demoted.makeMainProbation();
            accessOrderProbationDeque.offerLast(demoted);
            mainProtectedSize--;
        }

        this.mainProtectedSize = mainProtectedSize;
    }

    /**
     * Increases the size of the admission window by shrinking the portion allocated to the main
     * space. As the main space is partitioned into probation and protected regions (80% / 20%), for
     * simplicity only the protected is reduced. If the regions exceed their maximums, this may cause
     * protected items to be demoted to the probation region and probation items to be demoted to the
     * admission window.
     */
    @GuardedBy("evictionLock")
    private void increaseWindow() {
        if (mainProtectedMaximum == 0) {
            return;
        }

        long quota = Math.min(adjustment, mainProtectedMaximum);
        mainProtectedMaximum -= quota;
        windowMaximum += quota;

        demoteFromMainProtected();

        for (int i = 0; i < QUEUE_TRANSFER_THRESHOLD; i++) {
            Node candidate = accessOrderProbationDeque.peekFirst();
            boolean probation = true;

            if (candidate == null) {
                candidate = accessOrderProtectedDeque.peekFirst();
                probation = false;
            }

            if (candidate == null) {
                break;
            }

            quota--;
            if (probation) {
                accessOrderProbationDeque.remove(candidate);
            } else {
                mainProtectedSize--;
                accessOrderProtectedDeque.remove(candidate);
            }
            windowSize++;
            accessOrderWindowDeque.offerLast(candidate);
            candidate.makeWindow();
        }

        mainProtectedMaximum += quota;
        windowMaximum -= quota;

        adjustment = quota;
    }


    /** Calculates the amount to adapt the window by and sets {@link #adjustment} accordingly. */
    @GuardedBy("evictionLock")
    private void determineAdjustment() {
        int requestCount = hitsInSample + missesInSample;
        if (requestCount < frequencySketch.sampleSize) {
            return;
        }

        double hitRate = (double) hitsInSample / requestCount;
        double hitRateChange = hitRate - previousSampleHitRate;
        double amount = (hitRateChange >= 0) ? stepSize : -stepSize;
        double nextStepSize = (Math.abs(hitRateChange) >= HILL_CLIMBER_RESTART_THRESHOLD)
                ? HILL_CLIMBER_STEP_PERCENT * maximum * (amount >= 0 ? 1 : -1)
                : HILL_CLIMBER_STEP_DECAY_RATE * amount;

        previousSampleHitRate = hitRate;
        adjustment = (long) amount;
        stepSize = nextStepSize;

        missesInSample = 0;
        hitsInSample = 0;
    }


    /** Evicts entries if the cache exceeds the maximum. */
    @GuardedBy("evictionLock")
    private void evictEntries() {
        var candidate = evictFromWindow();
        evictFromMain(candidate);
    }

    /**
     * Evicts entries from the main space if the cache exceeds the maximum capacity. The main space
     * determines whether admitting an entry (coming from the window space) is preferable to retaining
     * the eviction policy's victim. This decision is made using a frequency filter so that the
     * least frequently used entry is removed.
     * <p>
     * The window space's candidates were previously promoted to the probation space at its MRU
     * position and the eviction policy's victim starts at the LRU position. The candidates are
     * evaluated in promotion order while an eviction is required, and if exhausted then additional
     * entries are retrieved from the window space. Likewise, if the victim selection exhausts the
     * probation space then additional entries are retrieved the protected space. The queues are
     * consumed in LRU order and the evicted entry is the one with a lower relative frequency, where
     * the preference is to retain the main space's victims versus the window space's candidates on a
     * tie.
     *
     * @param candidate the first candidate promoted into the probation space
     */
    @GuardedBy("evictionLock")
    private void evictFromMain(@Nullable Node candidate) {
        int victimQueue = PROBATION;
        int candidateQueue = PROBATION;
        Node victim = accessOrderProbationDeque.peekFirst();
        while (currentCacheSize > maximum) {
            // Search the admission window for additional candidates
            if ((candidate == null) && (candidateQueue == PROBATION)) {
                candidate = accessOrderWindowDeque.peekFirst();
                candidateQueue = WINDOW;
            }

            // Try evicting from the protected and window queues
            if ((candidate == null) && (victim == null)) {
                if (victimQueue == PROBATION) {
                    victim = accessOrderProtectedDeque.peekFirst();
                    victimQueue = PROTECTED;
                    continue;
                } else if (victimQueue == PROTECTED) {
                    victim = accessOrderWindowDeque.peekFirst();
                    victimQueue = WINDOW;
                    continue;
                }

                // The pending operations will adjust the size to reflect the correct weight
                break;
            }


            // Evict immediately if only one of the entries is present
            if (victim == null) {
                @SuppressWarnings("NullAway")
                Node previous = candidate.getNextInAccessOrder();
                Node evict = candidate;
                candidate = previous;

                evictEntry(evict);
                continue;
            } else if (candidate == null) {
                Node evict = victim;
                victim = victim.getNextInAccessOrder();
                evictEntry(evict);
                continue;
            }

            // Evict immediately if both selected the same entry
            if (candidate == victim) {
                victim = victim.getNextInAccessOrder();
                evictEntry(candidate);
                candidate = null;
                continue;
            }

            long victimKey = victim.getKey();
            long candidateKey = candidate.getKey();

            // Evict immediately if an entry was removed
            if (!victim.isAlive()) {
                Node evict = victim;
                victim = victim.getNextInAccessOrder();
                evictEntry(evict);
                continue;
            } else if (!candidate.isAlive()) {
                Node evict = candidate;
                candidate = candidate.getNextInAccessOrder();
                evictEntry(evict);
                continue;
            }

            // Evict the entry with the lowest frequency
            if (admit(candidateKey, victimKey)) {
                Node evict = victim;
                victim = victim.getNextInAccessOrder();
                evictEntry(evict);
                candidate = candidate.getNextInAccessOrder();
            } else {
                Node evict = candidate;
                candidate = candidate.getNextInAccessOrder();
                evictEntry(evict);
            }
        }
    }


    /**
     * Determines if the candidate should be accepted into the main space, as determined by its
     * frequency relative to the victim. A small amount of randomness is used to protect against hash
     * collision attacks, where the victim's frequency is artificially raised so that no new entries
     * are admitted.
     *
     * @param candidateKey the key for the entry being proposed for long term retention
     * @param victimKey the key for the entry chosen by the eviction policy for replacement
     * @return if the candidate should be admitted and the victim ejected
     */
    @GuardedBy("evictionLock")
    private boolean admit(long candidateKey, long victimKey) {
        int victimFreq = frequencySketch.frequency(victimKey);
        int candidateFreq = frequencySketch.frequency(candidateKey);
        if (candidateFreq > victimFreq) {
            return true;
        } else if (candidateFreq >= ADMIT_HASHDOS_THRESHOLD) {
            // The maximum frequency is 15 and halved to 7 after a reset to age the history. An attack
            // exploits that a hot candidate is rejected in favor of a hot victim. The threshold of a warm
            // candidate reduces the number of random acceptances to minimize the impact on the hit rate.
            int random = ThreadLocalRandom.current().nextInt();
            return ((random & 127) == 0);
        }
        return false;
    }

    /**
     * Attempts to evict the entry based on the given removal cause. A removal may be ignored if the
     * entry was updated and is no longer eligible for eviction.
     *
     * @param node the entry to evict
     */
    @GuardedBy("evictionLock")
    @SuppressWarnings({"GuardedByChecker", "NullAway", "PMD.CollapsibleIfStatements"})
    private void evictEntry(Node node) {
        long key = node.getKey();

        data.computeIfPresent(key + 1, (k, n) -> {
            if (n != node) {
                return n;
            }

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (n) {
                node.retire();
            }

            return null;
        });

        // If the eviction fails due to a concurrent removal of the victim, that removal may cancel out
        // the addition that triggered this eviction. The victim is eagerly unlinked and the size
        // decremented before the removal task so that if an eviction is still required then a new
        // victim will be chosen for removal.
        if (node.inWindow()) {
            accessOrderWindowDeque.remove(node);
        } else {
            if (node.inMainProbation()) {
                accessOrderProbationDeque.remove(node);
            } else {
                accessOrderProtectedDeque.remove(node);
            }
        }

        synchronized (node) {
            logIfAlive(node);
            makeDead(node);
        }
    }

    /**
     * Atomically transitions the node to the <tt>dead</tt> state and decrements the
     * <tt>weightedSize</tt>.
     *
     * @param node the entry in the page replacement policy
     */
    @GuardedBy("evictionLock")
    private void makeDead(Node node) {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (node) {
            if (node.isDead()) {
                return;
            }

            // The node's policy weight may be out of sync due to a pending update waiting to be
            // processed. At this point the node's weight is finalized, so the weight can be safely
            // taken from the node's perspective and the sizes will be adjusted correctly.
            if (node.inWindow()) {
                windowSize--;
            } else if (node.inMainProtected()) {
                mainProtectedSize--;
            }

            currentCacheSize--;
            node.die();

            addPageToFreePagesQueue(node);
        }
    }

    private void addPageToFreePagesQueue(Node node) {
        if (pagesVersionHandle.compareAndSet(pages, node.getValue(), node.pageVersion, node.pageVersion + 1)) {
            freePagesQueue.enqueue(node.getValue());
        } else {
            throw new IllegalStateException("Concurrent page eviction.");
        }
    }

    /** Logs if the node cannot be found in the map but is still alive. */
    private void logIfAlive(Node node) {
        if (node.isAlive()) {
            String message = brokenEqualityMessage(node.getKey());
            logger.error(message, new IllegalStateException());
        }
    }

    /** Returns the formatted broken equality error message. */
    private static String brokenEqualityMessage(long key) {
        return String.format(US, "An invalid state was detected, occurring when the key's equals or "
                + "hashCode was modified while residing in the cache. This violation of the Map "
                + "contract can lead to non-deterministic behavior (key: %s).", key);
    }

    /**
     * Evicts entries from the window space into the main space while the window size exceeds a
     * maximum.
     *
     * @return the first candidate promoted into the probation space
     */
    @GuardedBy("evictionLock")
    @Nullable
    private Node evictFromWindow() {
        Node first = null;
        Node node = accessOrderWindowDeque.peekFirst();

        while (windowSize > windowMaximum) {
            // The pending operations will adjust the size to reflect the correct weight
            if (node == null) {
                break;
            }

            Node next = node.getNextInAccessOrder();
            node.makeMainProbation();

            accessOrderWindowDeque.remove(node);
            accessOrderProbationDeque.offerLast(node);

            if (first == null) {
                first = node;
            }

            windowSize--;
            node = next;
        }

        return first;
    }


    /** Drains the write buffer. */
    @GuardedBy("evictionLock")
    private void drainWriteBuffer() {
        for (int i = 0; i <= ADD_BUFFER_MAX; i++) {
            Node node = addBuffer.poll();

            if (node == null) {
                return;
            }

            onAddTask(node);
        }

        setDrainStatusOpaque(PROCESSING_TO_REQUIRED);
    }

    /** Drains the read buffer. */
    @GuardedBy("evictionLock")
    private void drainReadBuffer() {
        readBuffer.drainTo(this::onAccess);
    }


    /** Updates the node's location in the page replacement policy. */
    @GuardedBy("evictionLock")
    private void onAccess(Node node) {
        var key = node.getKey();
        frequencySketch.increment(key);

        if (node.inWindow()) {
            reorder(accessOrderWindowDeque, node);
        } else if (node.inMainProbation()) {
            reorderProbation(node);
        } else {
            reorder(accessOrderProtectedDeque, node);
        }

        hitsInSample++;
    }


    /** Promote the node from probation to protected on an access. */
    @GuardedBy("evictionLock")
    private void reorderProbation(Node node) {
        if (!accessOrderProbationDeque.contains(node)) {
            // Ignore stale accesses for an entry that is no longer present
            return;
        }

        // If the protected space exceeds its maximum, the LRU items are demoted to the probation space.
        // This is deferred to the adaption phase at the end of the maintenance cycle.
        mainProtectedSize++;

        accessOrderProbationDeque.remove(node);
        accessOrderProtectedDeque.offerLast(node);

        node.makeMainProtected();
    }

    @Override
    public void close() throws IOException {
        arena.close();
        fileChannel.close();
    }

    /** Updates the node's location in the policy's deque. */
    private static void reorder(LinkedDeque<Node> deque, Node node) {
        // An entry may be scheduled for reordering despite having been removed. This can occur when the
        // entry was concurrently read while a writer was removing it. If the entry is no longer linked
        // then it does not need to be processed.
        if (deque.contains(node)) {
            deque.moveToBack(node);
        }
    }


    /** A reusable task that performs the maintenance work; used to avoid wrapping by ForkJoinPool. */
    private static final class PerformCleanupTask extends ForkJoinTask<Void> implements Runnable {
        private final WeakReference<DiskCache> reference;

        private PerformCleanupTask(DiskCache cache) {
            reference = new WeakReference<>(cache);
        }

        @Override
        public boolean exec() {
            try {
                run();
            } catch (Throwable t) {
                logger.error("Exception thrown when performing the maintenance task", t);
            }

            // Indicates that the task has not completed to allow subsequent submissions to execute
            return false;
        }

        @Override
        public void run() {
            var cache = reference.get();
            if (cache != null) {
                cache.performCleanUp(/* ignored */);
            }
        }

        /**
         * This method cannot be ignored due to being final, so a hostile user supplied Executor could
         * forcibly complete the task and halt future executions. There are easier ways to intentionally
         * harm a system, so this is assumed to not happen in practice.
         */
        // public final void quietlyComplete() {}
        @Override
        public Void getRawResult() {
            return null;
        }

        @Override
        public void setRawResult(Void v) {
        }

        @Override
        public void complete(Void value) {
        }

        @Override
        public void completeExceptionally(Throwable ex) {
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }
    }

    @NotNull
    private static PagesStructure calculatePagesStructure(long totalSize, int vectorDim,
                                                          int maxConnectionsPerVertex, Path graphFile) throws IOException {
        var pageStructure = calculatePageStructure(vectorDim, maxConnectionsPerVertex, graphFile);

        var cacheSize = 0.9 * totalSize;
        var cachePagesCount = cacheSize / pageStructure.pageSize;
        var blocksInPage = pageStructure.pageSize / pageStructure.blockSize;

        var preLoadersCount = (int) Math.max(4, 64 / blocksInPage);

        @SuppressWarnings("IntegerDivisionInFloatingPointContext")
        var preLoaderPagesCount = Math.max(totalSize / pageStructure.pageSize - cachePagesCount, preLoadersCount);

        var allocatedPagesCount = cachePagesCount + preLoaderPagesCount;

        if (allocatedPagesCount > Integer.MAX_VALUE) {
            allocatedPagesCount = Integer.MAX_VALUE;
            cachePagesCount = allocatedPagesCount - preLoaderPagesCount;
        }

        var pagesLayout = MemoryLayout.sequenceLayout((int) allocatedPagesCount, pageStructure.pageLayout);
        return new PagesStructure((int) cachePagesCount, preLoadersCount, (int) allocatedPagesCount,
                pageStructure, pagesLayout);
    }

    @NotNull
    public static PageStructure calculatePageStructure(int vectorDim, int maxConnectionsPerVertex, Path graphFile) throws IOException {
        var blockSize = (int) Files.getFileStore(graphFile).getBlockSize();
        var vertexLayout = MemoryLayout.structLayout(
                MemoryLayout.sequenceLayout(vectorDim, ValueLayout.JAVA_FLOAT).withName("vector"),
                MemoryLayout.sequenceLayout(maxConnectionsPerVertex, ValueLayout.JAVA_INT).withName("edges"),
                ValueLayout.JAVA_INT.withName("edgesCount")
        );

        var vertexRecordSize = (int) vertexLayout.byteSize();
        var minPageSize = Long.BYTES + vertexLayout.byteSize();
        int pageSize;

        int verticesCountPerPage;
        if (blockSize >= minPageSize) {
            verticesCountPerPage = ((blockSize - Long.BYTES) / vertexRecordSize);
            pageSize = blockSize;
        } else {
            //rounding to the closest disk page
            pageSize = blockSize *
                    ((Long.BYTES + vertexRecordSize + blockSize - 1) / blockSize);
            verticesCountPerPage = 1;
        }

        var paddingSpace = pageSize - (Long.BYTES + verticesCountPerPage * vertexRecordSize);
        var pageLayout = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("pageVersion"),
                MemoryLayout.sequenceLayout(verticesCountPerPage, vertexLayout).withName("vertices"),
                MemoryLayout.paddingLayout(8L * paddingSpace));
        assert pageSize == pageLayout.byteSize();

        var recordVectorOffset = (int) vertexLayout.byteOffset(MemoryLayout.PathElement.groupElement("vector"));
        var recordEdgesOffset = (int) vertexLayout.byteOffset(MemoryLayout.PathElement.groupElement("edges"));
        var recordEdgesCountOffset =
                (int) vertexLayout.byteOffset(MemoryLayout.PathElement.groupElement("edgesCount"));

        return new PageStructure(blockSize, pageSize, verticesCountPerPage, vertexRecordSize,
                recordVectorOffset, recordEdgesOffset, recordEdgesCountOffset, pageLayout);
    }

    private record PagesStructure(int cachePagesCount, int preLoadersCount, int allocatedPagesCount,
                                  PageStructure pageStructure, MemoryLayout pagesLayout) {
    }

    public record PageStructure(long blockSize, int pageSize, int verticesCountPerPage, int vertexRecordSize,
                                int recordVectorsOffset,
                                int recordEdgesOffset, int recordEdgesCountOffset, MemoryLayout pageLayout) {

    }
}

/** The namespace for field padding through inheritance. */
final class BLCHeader {

    @SuppressWarnings("unused")
    static class PadDrainStatus {
        byte p000, p001, p002, p003, p004, p005, p006, p007;
        byte p008, p009, p010, p011, p012, p013, p014, p015;
        byte p016, p017, p018, p019, p020, p021, p022, p023;
        byte p024, p025, p026, p027, p028, p029, p030, p031;
        byte p032, p033, p034, p035, p036, p037, p038, p039;
        byte p040, p041, p042, p043, p044, p045, p046, p047;
        byte p048, p049, p050, p051, p052, p053, p054, p055;
        byte p056, p057, p058, p059, p060, p061, p062, p063;
        byte p064, p065, p066, p067, p068, p069, p070, p071;
        byte p072, p073, p074, p075, p076, p077, p078, p079;
        byte p080, p081, p082, p083, p084, p085, p086, p087;
        byte p088, p089, p090, p091, p092, p093, p094, p095;
        byte p096, p097, p098, p099, p100, p101, p102, p103;
        byte p104, p105, p106, p107, p108, p109, p110, p111;
        byte p112, p113, p114, p115, p116, p117, p118, p119;
    }

    /** Enforces a memory layout to avoid false sharing by padding the drain status. */
    abstract static class DrainStatusRef extends BLCHeader.PadDrainStatus {
        static final VarHandle DRAIN_STATUS;

        /** A drain is not taking place. */
        static final int IDLE = 0;
        /** A drain is required due to a pending write modification. */
        static final int REQUIRED = 1;
        /** A drain is in progress and will transition to idle. */
        static final int PROCESSING_TO_IDLE = 2;
        /** A drain is in progress and will transition to required. */
        static final int PROCESSING_TO_REQUIRED = 3;

        /** The draining status of the buffers. */
        volatile int drainStatus = IDLE;

        /**
         * Returns whether maintenance work is needed.
         *
         * @param delayable if draining the read buffer can be delayed
         */
        boolean shouldDrainBuffers(boolean delayable) {
            return switch (drainStatusOpaque()) {
                case IDLE -> !delayable;
                case REQUIRED -> true;
                case PROCESSING_TO_IDLE, PROCESSING_TO_REQUIRED -> false;
                default -> throw new IllegalStateException("Invalid drain status: " + drainStatus);
            };
        }

        int drainStatusOpaque() {
            return (int) DRAIN_STATUS.getOpaque(this);
        }

        int drainStatusAcquire() {
            return (int) DRAIN_STATUS.getAcquire(this);
        }

        void setDrainStatusOpaque(int drainStatus) {
            DRAIN_STATUS.setOpaque(this, drainStatus);
        }

        void setDrainStatusRelease(@SuppressWarnings("SameParameterValue") int drainStatus) {
            DRAIN_STATUS.setRelease(this, drainStatus);
        }

        boolean casDrainStatus(int expect, int update) {
            return DRAIN_STATUS.compareAndSet(this, expect, update);
        }

        static {
            try {
                DRAIN_STATUS = MethodHandles.lookup()
                        .findVarHandle(BLCHeader.DrainStatusRef.class, "drainStatus", int.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }
}
