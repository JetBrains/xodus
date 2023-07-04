package jetbrains.exodus.newLogConcept.garbageCollector;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.operationLog.*;
import jetbrains.exodus.newLogConcept.transaction.TransactionState;
import jetbrains.exodus.newLogConcept.tree.TreeEntry;
import net.jpountz.xxhash.XXHash64;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_FACTORY;
import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_SEED;

public class OperationLogGarbageCollector {
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();

    public ConcurrentSkipListSet<TreeEntry> tree = new ConcurrentSkipListSet<>();

    public long findAllCommittedAndAbortedAndGetMaxId(Map<Long, OperationLogRecord> operationLog,
                                                      NonBlockingHashMapLong<SpecialRecordData> committedAndAbortedRecordsMap) {
        long lastCommittedAbortedId = -1L;
        for (var operationRecord : operationLog.entrySet()) {
            if (operationRecord.getValue().getLogRecordType() == LogRecordType.COMPLETION) {
            TransactionCompletionLogRecord compRecord = (TransactionCompletionLogRecord) operationRecord.getValue();
            var currentTxId = compRecord.getTransactionId();
                committedAndAbortedRecordsMap.put(currentTxId,
                        new SpecialRecordData(compRecord.getStatus(), operationRecord.getKey()));
                if (currentTxId > lastCommittedAbortedId){
                    lastCommittedAbortedId = currentTxId;
                }
            }
        }
        return lastCommittedAbortedId;
    }


    public void clean(Map<Long, OperationLogRecord> operationLog, NonBlockingHashMapLong<MVCCRecord> mvccHashMap) {
        NonBlockingHashMapLong<SpecialRecordData> committedAndAbortedRecordsMap = new NonBlockingHashMapLong();
        // first run - find all special records and create map of committed and aborted ids
        long lastCommittedAbortedId = findAllCommittedAndAbortedAndGetMaxId(operationLog, committedAndAbortedRecordsMap);
        // second run - remove/move all committed and aborted records and remove corresponding special records
        moveAndRemoveRecordsFromLog(operationLog, mvccHashMap, committedAndAbortedRecordsMap, lastCommittedAbortedId);
    }

    public void moveAndRemoveRecordsFromLog(Map<Long, OperationLogRecord> operationLog,
                                            NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                            NonBlockingHashMapLong<SpecialRecordData> committedAndAbortedRecordsMap,
                                            long lastCommittedAbortedId) {
        for (var operationRecord : operationLog.entrySet()) {
            if (operationRecord.getValue().getLogRecordType() == LogRecordType.OPERATION) {
                var committedOrAbortedRecord = committedAndAbortedRecordsMap.get(operationRecord.getValue().getTransactionId());

                if (committedOrAbortedRecord != null && committedOrAbortedRecord.state == TransactionState.REVERTED) {
                    logRemove(operationLog, operationRecord.getKey(), committedOrAbortedRecord.address);
                } else if (committedOrAbortedRecord != null && committedOrAbortedRecord.state == TransactionState.COMMITTED) {
                    TransactionOperationLogRecord record = (TransactionOperationLogRecord) operationRecord.getValue();
                    final long keyHashCode = xxHash.hash(record.key.getBaseBytes(), record.key.baseOffset(),
                            record.key.getLength(), XX_HASH_SEED);
                    MVCCRecord mvccRecord = mvccHashMap.compute(keyHashCode, (key, val) -> val);
                    if (mvccRecord == null) {
                        throw new ExodusException(); //todo replace it with "go search in tree" logic ?
                    }
                    var lastCommittedOperationReference = findLastCommitted(mvccRecord, keyHashCode, lastCommittedAbortedId);
                    assert lastCommittedOperationReference != null;
                    var targetTxId = operationRecord.getValue().getTransactionId();

                    removeCommittedRecord(lastCommittedOperationReference, targetTxId, operationRecord,
                            committedOrAbortedRecord, operationLog);
                }
            }
        }
    }

    private void moveToTree(ByteIterable key, ByteIterable value) {
        tree.add(new TreeEntry(key, value));
    }

    private void logRemove(Map<Long, OperationLogRecord> operationLog, long keyOperationRecord, long keySpecialRecord) {
        operationLog.remove(keyOperationRecord);
        operationLog.remove(keySpecialRecord);
    }

    private void removeCommittedRecord(OperationReference lastCommitted, long targetTxId,
                                       Map.Entry<Long, OperationLogRecord> operationRecord,
                                       SpecialRecordData committedOrAbortedRecord,
                                       Map<Long, OperationLogRecord> operationLog) {
        var lastCommittedTxId = lastCommitted.getTxId();
        if (lastCommitted.getTxId() == targetTxId) {
            // attention: do not move special records to the tree, this method is called for the operation
            // records only, therefore we can cast type here
            var rec = (TransactionOperationLogRecord)(operationRecord.getValue());
            ByteIterable value = rec.value;
            ByteIterable key = rec.key;
            moveToTree(key, value);
            logRemove(operationLog, operationRecord.getKey(), committedOrAbortedRecord.address);
        } else if (lastCommittedTxId > targetTxId) {
            logRemove(operationLog, operationRecord.getKey(), committedOrAbortedRecord.address);
        } else if (lastCommittedTxId < targetTxId) {
            throw new ExodusException();
        }
    }

    private OperationReference findLastCommitted(MVCCRecord mvccRecord, long keyHashCode, long lastCommittedAbortedId) {
        OperationReference lastCommittedOperationReference = null;
        // we add to queue several objects with one txID, the last one re-writes the previous value,
        // so we take the last with COMMITTED state
        for (var operationRef : mvccRecord.linksToOperationsQueue) {
            if (operationRef.getTxId() <= lastCommittedAbortedId && operationRef.keyHashCode == keyHashCode &&
                    operationRef.wrapper.state == TransactionState.COMMITTED.get()) {
                lastCommittedOperationReference = operationRef;
            }
        }
        return lastCommittedOperationReference;
    }
}
