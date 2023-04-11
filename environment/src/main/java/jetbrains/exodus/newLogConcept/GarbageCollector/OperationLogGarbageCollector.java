package jetbrains.exodus.newLogConcept.GarbageCollector;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.OperationLog.LogRecordType;
import jetbrains.exodus.newLogConcept.OperationLog.OperationLogRecord;
import jetbrains.exodus.newLogConcept.OperationLog.OperationReference;
import jetbrains.exodus.newLogConcept.OperationLog.TransactionOperationLogRecord;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import net.jpountz.xxhash.XXHash64;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.Map;

import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_FACTORY;
import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_SEED;

public class OperationLogGarbageCollector {
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();

    // todo where do we get targetTxId from
    void clean(Map<Long, OperationLogRecord> operationLog, NonBlockingHashMapLong<MVCCRecord> mvccHashMap, long targetTxId) {
        for (var operationRecord : operationLog.entrySet()) {
            if (operationRecord.getValue().getLogRecordType() == LogRecordType.OPERATION) {
                TransactionOperationLogRecord record = (TransactionOperationLogRecord) operationRecord.getValue();
                final long keyHashCode = xxHash.hash(record.key.getBaseBytes(), record.key.baseOffset(),
                        record.key.getLength(), XX_HASH_SEED);

                MVCCRecord mvccRecord = mvccHashMap.compute(keyHashCode, (key, val) -> val);
                if (mvccRecord == null) {
                    throw new ExodusException();
                }
                OperationReference lastCommittedOperationReference = null;
                for (var operationRef : mvccRecord.linksToOperationsQueue) {
                    if (operationRef.keyHashCode == keyHashCode && operationRef.wrapper.state == TransactionState.COMMITTED.get()) {
                        lastCommittedOperationReference = operationRef;
                    }
                }
                var lastCommittedTxId = lastCommittedOperationReference.getTxId();
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

    private void moveToBTree() {
        // todo not yet implemented
    }
}
