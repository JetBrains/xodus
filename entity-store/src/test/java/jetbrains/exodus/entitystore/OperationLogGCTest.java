package jetbrains.exodus.entitystore;

import jetbrains.exodus.newLogConcept.garbageCollector.OperationLogGarbageCollector;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.operationLog.OperationLogRecord;
import jetbrains.exodus.newLogConcept.operationLog.OperationReference;
import net.jpountz.xxhash.XXHash64;
import org.jctools.maps.NonBlockingHashMapLong;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_FACTORY;

public class OperationLogGCTest {
    private static final AtomicLong address = new AtomicLong(1);
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();

    @Test
    public void testCleanDeleteRecords() {
        final Map<Long, OperationLogRecord> operationLog = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            OperationReference reference1 = new OperationReference(address.get(), i, 1L);
            OperationReference reference2 = new OperationReference(address.getAndIncrement(), i+1, 1L);

            var queue = new ConcurrentLinkedQueue<OperationReference>();
            queue.add(reference1);
            queue.add(reference2);
            hashMap.put(i, new MVCCRecord(new AtomicLong(i), queue));

            String inputKey = "key-" + i; // Input string to convert

            // Convert string to byte array
            byte[] byteArray = inputKey.getBytes();

            // Create ByteIterable from byte array
            // TODO finish the tests
//            ByteIterable byteIterable = new ByteIterable(byteArray) {
//            };
//
//            var recordAddress = address.getAndIncrement();
//            final long keyHashCode = xxHash.hash(key.getBaseBytes(), key.baseOffset(), key.getLength(), XX_HASH_SEED);
//            var snapshot = transaction.getSnapshotId();
//            transaction.addOperationReferenceEntryToList(new OperationReference(recordAddress, snapshot, keyHashCode));
//            operationLog.put(recordAddress, new TransactionOperationLogRecord(key, value, operationType, snapshot));
        }

        var collector = new OperationLogGarbageCollector();
        collector.clean(operationLog, hashMap);

        Assert.assertTrue(hashMap.get(3L).linksToOperationsQueue.stream().anyMatch(it -> it.getTxId() == 4L));
        Assert.assertTrue(hashMap.get(2L).linksToOperationsQueue.stream().anyMatch(it -> it.getTxId() == 3L));
        Assert.assertFalse(hashMap.containsKey(1L));

    }
}
