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

import jetbrains.exodus.env.Environment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class TestTransactionReplayPersistentEntityStoreImpl extends PersistentEntityStoreImpl {

    private int replayFactor = 1;

    TestTransactionReplayPersistentEntityStoreImpl(@NotNull Environment environment, @NotNull String name) throws Exception {
        super(environment, name);
    }

    public TestTransactionReplayPersistentEntityStoreImpl(@NotNull Environment environment, @Nullable BlobVault blobVault, @NotNull String name) {
        super(environment, blobVault, name);
    }

    public int getReplayFactor() {
        return replayFactor;
    }

    public void setReplayFactor(int replayFactor) {
        this.replayFactor = replayFactor;
    }

    @NotNull
    @Override
    public PersistentStoreTransaction beginTransaction() {
        final PersistentStoreTransaction result = new PersistentStoreTransaction(this) {
            private int replayed = 0;

            @Override
            public boolean commit() {
                if (replayed++ < replayFactor) {
                    revert();
                    return false;
                }
                return super.commit();
            }

            @Override
            public boolean flush() {
                if (replayed++ < replayFactor) {
                    revert();
                    return false;
                }
                return super.flush();
            }
        };
        registerTransaction(result);
        return result;
    }
}
