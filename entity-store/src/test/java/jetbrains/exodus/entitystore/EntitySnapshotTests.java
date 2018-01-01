/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.JobProcessorExceptionHandler;
import jetbrains.exodus.core.execution.MultiThreadDelegatingJobProcessor;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.EnvironmentImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;

public class EntitySnapshotTests extends EntityStoreTestBase {

    @Override
    protected String[] casesThatDontNeedExplicitTxn() {
        return new String[]{"testConcurrentPutJetPassLike"};
    }

    public void testProperty() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity entity = txn.newEntity("Issue");
        entity.setProperty("name", "a");
        txn.flush();
        final PersistentStoreTransaction snap0 = txn.getSnapshot();
        entity.setProperty("name", "b");
        txn.flush();
        final PersistentStoreTransaction snap1 = txn.getSnapshot();
        entity.setProperty("name", "c");
        txn.flush();
        final ReadOnlyPersistentEntity v0 = entity.getSnapshot(snap0);
        final ReadOnlyPersistentEntity v1 = entity.getSnapshot(snap1);
        try {
            assertEquals("a", v0.getProperty("name"));
            assertEquals("b", v1.getProperty("name"));
        } finally {
            snap0.abort();
            snap1.abort();
        }
    }

    public void testBlob() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity entity = txn.newEntity("Issue");
        entity.setBlobString("name", "a");
        txn.flush();
        final PersistentStoreTransaction snap0 = txn.getSnapshot();
        entity.setBlobString("name", "b");
        txn.flush();
        final PersistentStoreTransaction snap1 = txn.getSnapshot();
        entity.setBlobString("name", "c");
        txn.flush();
        final ReadOnlyPersistentEntity v0 = entity.getSnapshot(snap0);
        final ReadOnlyPersistentEntity v1 = entity.getSnapshot(snap1);
        try {
            assertEquals("a", v0.getBlobString("name"));
            assertEquals("b", v1.getBlobString("name"));
        } finally {
            snap0.abort();
            snap1.abort();
        }
    }

    public void testLink() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final PersistentEntity entity = txn.newEntity("Issue");
        final PersistentEntity a = txn.newEntity("Comment");
        final PersistentEntity b = txn.newEntity("Comment");
        final PersistentEntity c = txn.newEntity("Comment");
        entity.setLink("comment", a);
        txn.flush();
        final PersistentStoreTransaction snap0 = txn.getSnapshot();
        entity.setLink("comment", b);
        txn.flush();
        final PersistentStoreTransaction snap1 = txn.getSnapshot();
        entity.setLink("comment", c);
        txn.flush();
        final ReadOnlyPersistentEntity v0 = entity.getSnapshot(snap0);
        final ReadOnlyPersistentEntity v1 = entity.getSnapshot(snap1);
        try {
            assertEquals(a, v0.getLink("comment"));
            assertEquals(b, v1.getLink("comment"));
        } finally {
            snap0.abort();
            snap1.abort();
        }
    }

    public void testConcurrentPutJetPassLike() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        getEntityStore().getConfig().setCachingDisabled(true);
        final EnvironmentImpl environment = (EnvironmentImpl) getEntityStore().getEnvironment();
        final EnvironmentConfig config = environment.getEnvironmentConfig();
        // disable GC
        config.setGcEnabled(false);
        final JobProcessor processor = new MultiThreadDelegatingJobProcessor("ConcurrentPutProcessor", 8) {
        };
        processor.setExceptionHandler(new JobProcessorExceptionHandler() {
            @Override
            public void handle(JobProcessor processor, Job job, Throwable t) {
                logger.error("Background exception", t);
            }
        });
        processor.start();
        final int count = 30000;
        for (int i = 0; i < count; ++i) {
            final int id = i;
            processor.queue(new Job() {
                @Override
                protected void execute() throws Throwable {
                    getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
                        @Override
                        public void execute(@NotNull final StoreTransaction txn) {
                            final Entity ticket = txn.newEntity("CASTicket");
                            ticket.setProperty("id", id);
                        }
                    });
                }
            });
        }
        processor.waitForJobs(100);
        processor.finish();
        getEntityStore().executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull final StoreTransaction txn) {
                //System.out.println("Structure id: " + executeMethod(environment, "getLastStructureId"));
                Assert.assertEquals(count, (int) txn.getAll("CASTicket").size());
                final EntityIterable sorted = txn.sort("CASTicket", "id", true);
                Assert.assertEquals(count, (int) sorted.size());
                int i = 0;
                for (final Entity ticket : sorted) {
                    final Comparable id = ticket.getProperty("id");
                    Assert.assertNotNull(id);
                    Assert.assertEquals(i++, id);
                }
            }
        });
    }
}
