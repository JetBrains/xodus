/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
import jetbrains.exodus.core.execution.ThreadJobProcessor;
import jetbrains.exodus.util.Random;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"QuestionableName", "ObjectAllocationInLoop"})
public class StressTests extends EntityStoreTestBase {

    public void testConcurrentRead() {
        final StoreTransaction txn = getStoreTransaction();
        createData(txn);
        ThreadJobProcessor[] procs = new ThreadJobProcessor[/*3*/2];
        for (int i = 0; i < procs.length; i++) {
            final int ii = i;
            ThreadJobProcessor proc = new ThreadJobProcessor("Processor" + 1);
            procs[i] = proc;
            proc.start();
            for (int j = 0; j < 10/*00*/; ++j) {
                final int jj = j;
                new Job(proc) {
                    @Override
                    protected void execute() throws Throwable {
                        final StoreTransaction s = getEntityStore().beginTransaction();
                        try {
                            final long now = System.currentTimeMillis();
                            s.find("Issue", "summary", "summary0", "summary" + (100 * ii + jj + 10000)).intersect(
                                    s.find("Issue", "created", now - 50000, now)).size();
                        } finally {
                            s.commit();
                        }
                    }
                };
            }
        }
        for (final ThreadJobProcessor proc : procs) {
            proc.queueFinish();
        }
        for (ThreadJobProcessor proc : procs) {
            proc.waitUntilFinished();
        }
    }

    public void test_xd_347_like() {
        final PersistentEntityStoreImpl store = getEntityStore();
        final List<Entity> issues = store.computeInTransaction(new StoreTransactionalComputable<List<Entity>>() {
            @Override
            public List<Entity> compute(@NotNull final StoreTransaction txn) {
                final List<Entity> result = new ArrayList<>();
                for (final Entity issue : txn.getAll("Issue")) {
                    result.add(issue);
                }
                while (result.size() < 10000) {
                    result.add(txn.newEntity("Issue"));
                }
                return result;
            }
        });
        final Random rnd = new Random();
        final int count = 300;
        for (int i = 0; i < count; ++i) {
            store.executeInTransaction(new StoreTransactionalExecutable() {
                @Override
                public void execute(@NotNull final StoreTransaction txn) {
                    for (int j = 0; j < 100; ++j) {
                        issues.get(rnd.nextInt(issues.size())).setProperty("created", System.currentTimeMillis());
                        issues.get(rnd.nextInt(issues.size())).setProperty("updated", System.currentTimeMillis());
                    }
                }
            });
            if (i % 10 == 0) {
                System.out.print((i * 100 / count) + "%\r");
            }
        }
    }

    private static void createData(final StoreTransaction txn) {
        for (int i = 0; i < 100000; ++i) {
            final Entity issue = txn.newEntity("Issue");
            issue.setProperty("summary", "summary" + i);
            issue.setProperty("created", System.currentTimeMillis());
            if (i % 1000 == 999) {
                txn.flush();
            }
        }
        txn.flush();
    }
}
