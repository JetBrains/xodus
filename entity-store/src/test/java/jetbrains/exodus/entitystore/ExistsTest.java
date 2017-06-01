package jetbrains.exodus.entitystore;

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.env.Transaction;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExistsTest {
  public static final String STORE_NAME = "testStore";

  @Test
  public void testStoreExists() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));

    String path;
    File tempDir = new File(baseDir, String.valueOf(System.currentTimeMillis()));
    if (tempDir.mkdir()) {
      path  = tempDir.getAbsolutePath();
    } else {
      path = null;
      fail();
    }

    Environment env = Environments.newInstance(path, EnvironmentConfig.DEFAULT);
    Transaction tx = env.beginExclusiveTransaction();
    Assert.assertFalse(env.storeExists(STORE_NAME, tx));
    if (!tx.commit()) {
      throw new IllegalStateException("Couldn't commit store test transaction");
    }

    PersistentEntityStore store = PersistentEntityStores.newInstance(env, STORE_NAME);

    StoreTransaction txd = store.beginTransaction();
    Entity testEntity = txd.newEntity("testEntity");
    EntityId id = testEntity.getId();
    testEntity.setProperty("testProperty", 42);
    txd.saveEntity(testEntity);
    txd.commit();
    store.close();

    Environment env1 = Environments.newInstance(path, EnvironmentConfig.DEFAULT);
    Transaction tx1 = env1.beginExclusiveTransaction();
    boolean storeExists = env1.storeExists(STORE_NAME, tx1);
    if (!tx1.commit()) {
      throw new IllegalStateException("Couldn't commit store test transaction");
    }

    PersistentEntityStore store1 = PersistentEntityStores.newInstance(env1, STORE_NAME);
    StoreTransaction txd1 = store1.beginTransaction();
    Entity entity1 = txd1.getEntity(id);
    assertEquals(42, entity1.getProperty("testProperty"));
    assertTrue(storeExists);
  }
}
