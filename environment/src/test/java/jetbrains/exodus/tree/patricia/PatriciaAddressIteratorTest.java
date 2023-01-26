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
package jetbrains.exodus.tree.patricia;

import org.junit.Test;

public class PatriciaAddressIteratorTest extends PatriciaTestBase {

    @Test
    public void testSimple() {
        tm = createMutableTree(false, 1);
        for (char c = 'a'; c < 'k'; c++) {
            getTreeMutable().put(kv("xx" + c, ""));
        }

        for (char c = 'a'; c < 'k'; c++) {
            getTreeMutable().put(kv("xy" + c, ""));
        }

        t = new PatriciaTree(log, saveTree(), 1);
        checkAddressSet(getTree(), 23);
    }
}
