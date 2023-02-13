/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.lucene2;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestRuleLimitSysouts;

import java.io.IOException;
import java.nio.file.Path;

@ThreadLeakFilters(filters = XodusThreadFilter.class)
@TestRuleLimitSysouts.Limit(bytes = 20 * 1024)
public class XodusDirectoryTest extends BaseDirectoryTestCase {
    @Override
    protected Directory getDirectory(Path path) throws IOException {
        final EnvironmentConfig config = new EnvironmentConfig();
        config.setLogCachePageSize(1024);

        Environment environment = Environments.newInstance(path.toFile(), config);

        return new XodusDirectory(environment);
    }
}
