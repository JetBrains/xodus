/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.tree.ITree;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;
import java.util.function.Function;

public class BasicBTreeTest extends BTreeTestBase {
    @Test
    public void checkEmptyTree() {
        createMutableTree(false, 1);

        long address = saveTree();
        var t = openTree(address, false);

        checkEmptyTree(t);

        reopen();

        t = openTree(address, false);
        checkEmptyTree(t);
    }

    @Test
    public void singlePutGet() {
        var tm = createMutableTree(false, 1);
        tm.put(key(1), value("v1"));

        checkTree(false, t -> {
            Assert.assertEquals(1, t.getSize());
            Assert.assertFalse(t.isEmpty());

            Assert.assertEquals(value("v1"), t.get(key(1)));
            Assert.assertTrue(t.hasKey(key(1)));
            Assert.assertTrue(t.hasPair(key(1), value("v1")));

            Assert.assertNull(t.get(key(2)));
            Assert.assertFalse(t.hasPair(key(1), value("v2")));
            Assert.assertFalse(t.hasPair(key(2), value("v1")));
            Assert.assertFalse(t.hasPair(key(2), value("v2")));
            Assert.assertFalse(t.hasKey(key(2)));
        });
    }
}
