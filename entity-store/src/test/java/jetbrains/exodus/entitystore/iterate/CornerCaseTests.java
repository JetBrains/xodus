/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

public class CornerCaseTests extends EntityStoreTestBase {

    @Override
    protected boolean needsImplicitTxn() {
        return false;
    }

    public void testSubsystemsSort() {
        final PersistentEntityStoreImpl store = getEntityStore();
        EntityStoreSharedAsyncProcessor asyncProcessor = store.getAsyncProcessor();
        asyncProcessor.waitForJobs(100);
        asyncProcessor.finish();
        store.getConfig().setCachingDisabled(true);
        for (int i = 0; i < 100; i++) {
            try {
                store.executeInTransaction(new StoreTransactionalExecutable() {
                    @Override
                    public void execute(@NotNull StoreTransaction txn) {
                        Entity defaultSubsystem = txn.newEntity("Subsystem");
                        Entity project = txn.newEntity("Project");
                        project.setLink("defaultSubsystem", defaultSubsystem);
                        addSubsystem(project, defaultSubsystem, "Unknown");
                        addSubsystem(project, txn.newEntity("Subsystem"), "s3");
                        addSubsystem(project, txn.newEntity("Subsystem"), "s1");
                        addSubsystem(project, txn.newEntity("Subsystem"), "s2");
                        Assert.assertEquals("s1", doSort(txn, project).getFirst().getProperty("name"));
                        Assert.assertEquals("Unknown", doSort(txn, project).getLast().getProperty("name"));
                    }
                });
            } catch (Throwable t) {
                System.out.println("Failed at iteration " + i);
                throw t;
            }
        }
    }

    private EntityIterable doSort(StoreTransaction txn, Entity project) {
        return txn.sort("Subsystem", "name", project.getLinks("subsystems"), true).asSortResult();
    }

    private void addSubsystem(Entity project, Entity subsystem, String name) {
        subsystem.setProperty("name", name);
        project.addLink("subsystems", subsystem);
        subsystem.setLink("parent", project);
    }
}
