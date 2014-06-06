/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import jetbrains.exodus.bindings.StringBinding;
import org.jetbrains.annotations.NotNull;

/**
 * This example shows low level key/value store access.
 */
public class EnvironmentReadWriteExample {

    public static void main(String[] args) {

        //Create environment or open existing one
        final Environment env = Environments.newInstance("data", EnvironmentConfig.DEFAULT);

        //Create or open existing store in environment
        final Store store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
                return env.openStore("MyStore", StoreConfiguration.WITHOUT_DUPLICATES, txn);
            }
        });

        // Put "myValue" string under the key "myKey"
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, StringBinding.stringToEntry("myKey"), StringBinding.stringToEntry("myValue"));
            }
        });

        // Read value by key "myKey"
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                System.out.println(StringBinding.entryToString(store.get(txn, StringBinding.stringToEntry("myKey"))));
            }
        });

        // Close environment when we are done
        env.close();
    }
}
