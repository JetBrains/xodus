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

import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;

import static jetbrains.exodus.entitystore.StressTests.xd_347_like;

public class SmallLogStressTests extends EntityStoreTestBase {
    private static final int LOG_SIZE = 4; // kb

    @Override
    protected PersistentEntityStoreImpl createStoreInternal(String dbTempFolder) {
        EnvironmentConfig cfg = new EnvironmentConfig().setLogFileSize(LOG_SIZE).setLogCachePageSize(LOG_SIZE * 1024).setLogCacheShared(false);
        return PersistentEntityStores.newInstance(Environments.newInstance(dbTempFolder, cfg));
    }

    public void test_xd_347_like() {
        xd_347_like(getEntityStore(), 30);
    }
}
