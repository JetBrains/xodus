package jetbrains.exodus.newLogConcept;
import java.util.concurrent.CountDownLatch;
public class TransactionStateWrapper {
    TransactionState state; // todo replace to reference to object in hashmap

}

class TransactionInformation {
    TransactionStateWrapper state;
    CountDownLatch operationsCountLatch; // todo: create only if num of operations is 10+

}

