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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.bindings.StringBinding.entryToString;
import static jetbrains.exodus.bindings.StringBinding.stringToEntry;
import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES;

/**
 * This example shows low level key/value store access.
 */
public class EnvironmentReadWriteExample {

    public static void main(String[] args) {

        //Create environment or open existing one
        final Environment env = Environments.newInstance("data");

        //Create or open existing store in environment
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
                return env.openStore("MyStore", WITHOUT_DUPLICATES, txn);
            }
        });

        @NotNull final ByteIterable key = stringToEntry("myKey");
        @NotNull final ByteIterable value = stringToEntry("myValue");

        // Put "myValue" string under the key "myKey"
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, key, value);
            }
        });

        // Read value by key "myKey"
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final ByteIterable entry = store.get(txn, key);
                assert entry == value;
                System.out.println(entryToString(entry));
            }
        });

        // Close environment when we are done
        env.close();
    }
}
