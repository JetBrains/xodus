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

import jetbrains.exodus.TestFor;
import jetbrains.exodus.bindings.ComparableSet;

public class PersistentEntityStoreRefactoringsTests extends EntityStoreTestBase {

    @TestFor(issues = "XD-553", testForClass = PersistentEntityStoreRefactorings.class)
    public void testConsistentPropsRefactoringForComparableSetValues() {

        // at first, disable entity iterables' caching
        getEntityStore().getConfig().setCachingDisabled(true);

        final PersistentStoreTransaction txn = getStoreTransaction();

        final PersistentEntity user = txn.newEntity("User");
        user.setProperty("login", "user");
        final ComparableSet<String> set = new ComparableSet<>();
        set.addItem("user@jetbrains.com");
        set.addItem("user@intellij.net");
        user.setProperty("email", set);

        txn.flush();

        Comparable[] emails = set.toArray();
        for (final Comparable email : emails) {
            assertEquals(user, txn.find("User", "email", email).getFirst());
        }

        // run refactoring
        new PersistentEntityStoreRefactorings(getEntityStore()).refactorMakePropTablesConsistent();

        // move txn to the latest snapshot
        txn.revert();

        emails = set.toArray();
        for (final Comparable email : emails) {
            assertEquals(user, txn.find("User", "email", email).getFirst());
        }
    }
}
