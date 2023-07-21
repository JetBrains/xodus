package site.ycsb.db;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import jetbrains.exodus.newLogConcept.MVCC.MVCCDataStructure;
import site.ycsb.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Client class for the Xodus DB.
 **/
public class XodusClient extends DB {

  private static final Lock INIT_LOCK = new ReentrantLock();
  private static final ThreadLocal<Environment> ENV_THREAD_LOCAL = new ThreadLocal<>();
  private static final ThreadLocal<Store> STORE_THREAD_LOCAL = new ThreadLocal<>();
  private static final String STORE_NAME = "store";

  //  private static final String DB_PATH = "/home/alinaboshchenko/.myAppData";
  private static final String DB_PATH = "/home/alinaboshchenko/WorkJB/benchmarking-experimental/YCSB/data";

  @Override
  public void init() {
    Environment env = ENV_THREAD_LOCAL.get();
    Store store = STORE_THREAD_LOCAL.get();
    if (env == null || store == null) {
      INIT_LOCK.lock();
      try {
        if (env == null || store == null) {
          env = Environments.newInstance(DB_PATH); // error here
          Environment finalEnv = env;
          store = env.computeInTransaction(txn -> finalEnv.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));
          ENV_THREAD_LOCAL.set(env);
          STORE_THREAD_LOCAL.set(store);
        }
      } catch (ExodusException e){
        try {
          if (env == null || store == null) {
            env = Environments.newInstance(DB_PATH + "1"); // error here
            Environment finalEnv = env;
            store = env.computeInTransaction(txn ->
                finalEnv.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));
            ENV_THREAD_LOCAL.set(env);
            STORE_THREAD_LOCAL.set(store);
          }
        } catch (ExodusException e2){
          try {
            if (env == null || store == null) {
              env = Environments.newInstance(DB_PATH + "2"); // error here
              Environment finalEnv = env;
              store = env.computeInTransaction(txn ->
                  finalEnv.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));
              ENV_THREAD_LOCAL.set(env);
              STORE_THREAD_LOCAL.set(store);
            }
          } catch (ExodusException e3){
            try {
              if (env == null || store == null) {
                env = Environments.newInstance(DB_PATH + "3"); // error here
                Environment finalEnv = env;
                store = env.computeInTransaction(txn ->
                    finalEnv.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));
                ENV_THREAD_LOCAL.set(env);
                STORE_THREAD_LOCAL.set(store);
              }
            } catch (ExodusException e4){
              System.out.println("Error");
            }
          }
        }
      } finally {
        INIT_LOCK.unlock();
      }
    }
  }

  @Override
  public void cleanup() {
    Environment env = ENV_THREAD_LOCAL.get();
    Store store = STORE_THREAD_LOCAL.get();
    if (env != null && store != null) {
      store.close();
      env.close();
      ENV_THREAD_LOCAL.remove();
      STORE_THREAD_LOCAL.remove();
    }
  }

  // TODO: not sure ab this: key = The record key of the record to insert.
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Environment env = ENV_THREAD_LOCAL.get();
    Store store = STORE_THREAD_LOCAL.get();
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      env.executeInTransaction(txn -> {
          store.put(txn, StringBinding.stringToEntry(key),
              StringBinding.stringToEntry(byteIteratorToString(value.getValue())));
        });
    }
//    env.executeInTransaction(txn -> {
//        for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
//          store.put(txn, StringBinding.stringToEntry(value.getKey()),
//              StringBinding.stringToEntry(byteIteratorToString(value.getValue())));
//        }
//      });
    return Status.OK;
  }

  private String byteIteratorToString(ByteIterator byteIter) {
    return new String(byteIter.toArray());
  }

  // todo fix returns
  @Override
  public Status delete(String table, String key) {
    Environment env = ENV_THREAD_LOCAL.get();
    Store store = STORE_THREAD_LOCAL.get();
    env.executeInTransaction(txn -> store.delete(txn, StringBinding.stringToEntry(key)));
    return Status.OK;
  }

  // TODO: not sure ab this: key = The record key of the record to insert.
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
//    env.executeInTransaction(txn -> {
//      // todo
//        ByteIterable value = store.get(txn, StringBinding.stringToEntry(key));
//        assert value != null;
//      });
////    env.close();
//    return Status.OK;
    Environment env = ENV_THREAD_LOCAL.get();
    Store store = STORE_THREAD_LOCAL.get();
    env.executeInReadonlyTransaction(txn -> {
        final ByteIterable valueEntry = store.get(txn, StringBinding.stringToEntry(key));
        assert valueEntry != null;
      });
    return Status.OK;

  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // todo ?
    return insert(table, key, values);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    //  we are not interested in the range query benchmarking for now
    // todo not yet implemented
    return Status.OK;
  }
}
