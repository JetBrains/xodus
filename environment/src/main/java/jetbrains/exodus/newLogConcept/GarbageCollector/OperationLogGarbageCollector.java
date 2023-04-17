package jetbrains.exodus.newLogConcept.GarbageCollector;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.OperationLog.*;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import net.jpountz.xxhash.XXHash64;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.Map;

import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_FACTORY;
import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_SEED;

public class OperationLogGarbageCollector {
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();

    public void findAllCommittedAndAborted(Map<Long, OperationLogRecord> operationLog,
                                           NonBlockingHashMapLong<TransactionState> committedAndAbortedRecordsMap) {
        for (var operationRecord : operationLog.entrySet()) {
            TransactionCompletionLogRecord compRecord = (TransactionCompletionLogRecord) operationRecord.getValue();
            if (compRecord.getLogRecordType() == LogRecordType.COMPLETION) {
                committedAndAbortedRecordsMap.put(compRecord.getTransactionId(), compRecord.getStatus());
            }
        }
    }


    public void clean(Map<Long, OperationLogRecord> operationLog, NonBlockingHashMapLong<MVCCRecord> mvccHashMap) {
        NonBlockingHashMapLong<TransactionState> committedAndAbortedRecordsMap = new NonBlockingHashMapLong();
        // first run - find all special records and create map of committed and aborted ids
        findAllCommittedAndAborted(operationLog, committedAndAbortedRecordsMap);
        moveAndRemoveRecordsFromLog(operationLog, mvccHashMap, committedAndAbortedRecordsMap);


    }

    // todo where do we get targetTxId from
    public void moveAndRemoveRecordsFromLog(Map<Long, OperationLogRecord> operationLog,
                                            NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                            NonBlockingHashMapLong<TransactionState> committedAndAbortedRecordsMap) {
        for (var operationRecord : operationLog.entrySet()) {
            if (operationRecord.getValue().getLogRecordType() == LogRecordType.OPERATION) {

                // todo remove special records as well
                if (committedAndAbortedRecordsMap.get(operationRecord.getKey()) == TransactionState.REVERTED) {
                    operationLog.remove(operationRecord.getKey());
                } else if (committedAndAbortedRecordsMap.get(operationRecord.getKey()) == TransactionState.COMMITTED) {
                    TransactionOperationLogRecord record = (TransactionOperationLogRecord) operationRecord.getValue();
                    final long keyHashCode = xxHash.hash(record.key.getBaseBytes(), record.key.baseOffset(),
                            record.key.getLength(), XX_HASH_SEED);

                    MVCCRecord mvccRecord = mvccHashMap.compute(keyHashCode, (key, val) -> val);
                    if (mvccRecord == null) {
                        throw new ExodusException();
                    }
                    // todo is that exactly last committed? check
                    OperationReference lastCommittedOperationReference = null;
                    for (var operationRef : mvccRecord.linksToOperationsQueue) {
                        if (operationRef.keyHashCode == keyHashCode && operationRef.wrapper.state == TransactionState.COMMITTED.get()) {
                            lastCommittedOperationReference = operationRef;
                        }
                    }
                    assert lastCommittedOperationReference != null;
                    var lastCommittedTxId = lastCommittedOperationReference.getTxId();
                    var targetTxId = operationRecord.getKey();
                    if (lastCommittedOperationReference.getTxId() == targetTxId) {
                        moveToBTree();
                        operationLog.remove(operationRecord.getKey());
                    } else if (lastCommittedTxId > targetTxId) {
                        operationLog.remove(operationRecord.getKey());
                    } else if (lastCommittedTxId < targetTxId) {
                        throw new ExodusException();
                    }
                }
            }
        }
    }

    private void moveToBTree() {
        // todo not yet implemented
    }
}
