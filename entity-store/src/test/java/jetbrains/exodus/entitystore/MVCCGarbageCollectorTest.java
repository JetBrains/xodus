package jetbrains.exodus.entitystore;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.newLogConcept.garbageCollector.MVCCGarbageCollector;
import jetbrains.exodus.newLogConcept.garbageCollector.TransactionGCEntry;
import jetbrains.exodus.newLogConcept.MVCC.MVCCDataStructure;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.operationLog.OperationReference;
import jetbrains.exodus.newLogConcept.transaction.Transaction;
import jetbrains.exodus.newLogConcept.transaction.TransactionState;
import net.jpountz.xxhash.XXHash64;
import org.jctools.maps.NonBlockingHashMapLong;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_FACTORY;


// TODO fixme tests - rewrite tests to match the changed logic of the MVCC GC with operations
public class MVCCGarbageCollectorTest {
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();


    // TODO fixme - unlock collector in MVCCComponent and fix assertion error
    @Test
    public void performanceGCWith1_000_000NewTransactionsPerIncrementTest() throws ExecutionException, InterruptedException {
        ExecutorService service = Executors.newCachedThreadPool();
        Map<String, String> keyValTransactions = new HashMap<>();
        var mvccComponent = new MVCCDataStructure();

        String keyString = "key-" + (int) (Math.random() * 100000);
        AtomicLong value = new AtomicLong(1000);
        keyValTransactions.put(keyString, String.valueOf(value));

        for (int i = 0; i < 12; i++) {
            for (int j = 0; j < 1_000; j++) { // todo: replace with 1_000_000, for this first come up with the GC for OL (tested on mock
                var th = service.submit(() -> {
                    Transaction writeTransaction = mvccComponent.startWriteTransaction();
                    // check record is null before the commit
                    mvccComponent.put(writeTransaction, StringBinding.stringToEntry(keyString),
                            StringBinding.stringToEntry(String.valueOf(value)));

                    Assert.assertEquals(writeTransaction.getSnapshotId(), writeTransaction.getOperationLinkList().get(0).getTxId());
                    try {
                        mvccComponent.commitTransaction(writeTransaction);
                    } catch (ExecutionException | InterruptedException e) {
                        throw new ExodusException(e);
                    }
                });
                th.get();
            }
            value.getAndIncrement();
        }
        Assert.assertEquals(1012, value.get());
    }


    @Test
    public void testCleanDeleteRecords() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            OperationReference reference1 = new OperationReference(1L, i, 1L);
            OperationReference reference2 = new OperationReference(1L, i+1, 1L);

            var queue = new ConcurrentLinkedQueue<OperationReference>();
            queue.add(reference1);
            queue.add(reference2);
            hashMap.put(i, new MVCCRecord(new AtomicLong(i), queue));

            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(5L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.get(3L).linksToOperationsQueue.stream().anyMatch(it -> it.getTxId() == 4L));
        Assert.assertTrue(hashMap.get(2L).linksToOperationsQueue.stream().anyMatch(it -> it.getTxId() == 3L));
        Assert.assertFalse(hashMap.containsKey(1L));

    }

    @Test
    @Ignore
    public void testCleanWithAllCommittedOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(7L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(6L));

        for (long i = 1; i < 6; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
    }

    @Test
    @Ignore
    public void testCleanWithMissingIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 6; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(3L));
        Assert.assertTrue(hashMap.containsKey(6L));

        for (long i = 1; i < 3; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        for (long i = 4; i < 6; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }

    }

    @Test
    public void testCleanWithMergedIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 4; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        TransactionGCEntry entry = new TransactionGCEntry(TransactionState.COMMITTED.get(), 6);
        transactionsGCMap.put(4L, entry);

        for (long i = 7; i < 8; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);
        for (long i = 1; i < 7; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        // todo VERIFY
        Assert.assertFalse(hashMap.containsKey(7L)); //as linksToOperationsQueue.isEmpty() -> remove maxMinID record as well
    }

    // TODO add elements to the queue for the test to be complete in terms of logic
    @Test
    @Ignore
    public void testCleanWithMergedIdsAndMissedValueOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        for (long i = 5; i < 6; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        TransactionGCEntry entry = new TransactionGCEntry(TransactionState.COMMITTED.get(), 6);
        transactionsGCMap.put(5L, entry);
        for (long i = 7; i < 8; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        Assert.assertFalse(hashMap.containsKey(1L));
        Assert.assertFalse(hashMap.containsKey(2L));
        Assert.assertTrue(hashMap.containsKey(3L));
        Assert.assertFalse(hashMap.containsKey(4L));
        Assert.assertTrue(hashMap.containsKey(5L));
        Assert.assertFalse(hashMap.containsKey(6L));
        Assert.assertFalse(hashMap.containsKey(7L));

        Assert.assertEquals(collector.findMaxMinId(transactionsGCMap, 10L).longValue(), 3L);
    }


    @Test
    public void testCleanWithNoMaxMinId() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(5L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(6L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 7L));
        hashMap.put(5L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        hashMap.put(6L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        for (long i = 8; i < 9; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(5L));
        Assert.assertTrue(hashMap.containsKey(6L));
        Assert.assertTrue(hashMap.containsKey(8L));

    }

    @Test
    @Ignore
    public void testCleanWithoutMissingIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 4; i < 6; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 6; i < 7; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        Assert.assertFalse(hashMap.containsKey(1L));
        Assert.assertFalse(hashMap.containsKey(2L));
        for (long i = 3; i < 7; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
    }

    @Test
    @Ignore
    public void testCleanWithRevertedIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 4; i < 6; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 6; i < 7; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        for (long i = 1; i < 6; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(6L));
    }


    @Test
    @Ignore
    public void testCleanWithActiveTransactionsIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 4; i < 7; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        for (long i = 7; i < 10; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(15L, hashMap, transactionsGCMap);

        for (long i = 1; i < 3; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        for (long i = 4; i < 7; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
        for (long i = 8; i < 10; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(7L));
    }

    @Test
    @Ignore
    public void getCleanWithMissingIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 5; i < 6; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        for (long i = 10; i < 11; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(11L, hashMap, transactionsGCMap);

        for (long i = 1; i < 3; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(3L));
        Assert.assertTrue(hashMap.containsKey(5L));
        Assert.assertTrue(hashMap.containsKey(10L));
    }

    @Test
    @Ignore
    public void testCleanMissingTransactionsIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 5; i < 6; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 6; i < 8; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 9; i < 11; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        for (long i = 13; i < 20; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(15L, hashMap, transactionsGCMap);

        for (long i = 1; i < 3; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(3L));
        for (long i = 5; i < 8; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(9L));
        Assert.assertFalse(hashMap.containsKey(10L));
        for (long i = 13; i < 15; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
    }

    @Test
    @Ignore
    public void testActiveAndMissingTransactionsIdsWithUpToOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(1L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 2L));
        hashMap.put(1L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        transactionsGCMap.put(3L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 6L));
        hashMap.put(3L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        for (long i = 7; i < 8; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 9; i < 11; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        transactionsGCMap.put(12L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 14L));
        hashMap.put(12L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        for (long i = 16; i < 20; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(18L, hashMap, transactionsGCMap);

        Assert.assertFalse(hashMap.containsKey(1L));
        Assert.assertTrue(hashMap.containsKey(3L));
        Assert.assertTrue(hashMap.containsKey(7L));
        Assert.assertTrue(hashMap.containsKey(9L));
        Assert.assertFalse(hashMap.containsKey(10L));
        Assert.assertTrue(hashMap.containsKey(12L));
        for (long i = 16; i < 18; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
    }


    @Test
    public void testCleanWithNoMaxMinIdOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        for (long i = 1; i < 7; i++) {
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        hashMap.put(9L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        transactionsGCMap.put(1L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.REVERTED.get()));
        transactionsGCMap.put(3L, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        transactionsGCMap.put(4L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(5L, new TransactionGCEntry(TransactionState.REVERTED.get()));
        transactionsGCMap.put(6L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 8L));
        transactionsGCMap.put(9L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        for (long i = 1; i < 7; i++) {
            Assert.assertTrue(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(9L));
    }

    @Test
    public void testCleanWithNoMaxMinIdOneThread2() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 6; i++) {
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        transactionsGCMap.put(1L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.REVERTED.get()));
        transactionsGCMap.put(3L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(4L, new TransactionGCEntry(TransactionState.REVERTED.get()));
        transactionsGCMap.put(5L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        for (long i = 1; i < 6; i++) {
            Assert.assertTrue(hashMap.containsKey(i));
        }
    }

    @Test
    @Ignore
    public void testCleanAllCommittedOrRevertedOneThread2() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 4; i < 7; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(7L, hashMap, transactionsGCMap);

        for (long i = 1; i < 6; i++) {
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(6L));
    }


    @Test
    @Ignore
    public void testCleanWithAllCommittedAndMergedOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 4L));
        transactionsGCMap.put(5L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 6L));

        hashMap.put(2L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        hashMap.put(5L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        var collector = new MVCCGarbageCollector();
        collector.clean(7L, hashMap, transactionsGCMap);

        for (long i = 1; i < 5; i++) {
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(5L));
    }

    @Test
    @Ignore
    public void testCleanWithAllMergedOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 7L));
        hashMap.put(2L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        var collector = new MVCCGarbageCollector();
        collector.clean(6L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(2L));

        for (long i = 3; i < 6; i++) {
            Assert.assertFalse(hashMap.containsKey(i));
        }
    }

    @Test
    public void testCleanWithAlmostAllMergedOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 7L));
        transactionsGCMap.put(8L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        hashMap.put(2L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        hashMap.put(8L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        var collector = new MVCCGarbageCollector();
        collector.clean(9L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(2L));
        Assert.assertTrue(hashMap.containsKey(8L));

        for (long i = 3; i < 8; i++) {
            Assert.assertFalse(hashMap.containsKey(i));
        }
    }
}
