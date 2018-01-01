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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.LinkedHashSet;
import jetbrains.exodus.tree.INode;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class BTreeLeafNodeTest extends BTreeTestBase {

    @Test
    public void testEquals() {
        assertEquals(kv(1, "v11"), kv(1, "v12"));
        assertEquals(kv(1, "v11").hashCode(), kv(1, "v12").hashCode());
    }

    @Test
    public void testSet() {
        Set<INode> set = new HashSet<>();

        set.add(kv(1, "v11"));
        set.add(kv(1, "v12"));

        assertEquals(1, set.size());
    }

    @Test
    public void testSet2() {
        Set<INode> set = new LinkedHashSet<>();

        set.add(kv(1, "v11"));
        set.add(kv(1, "v12"));

        assertEquals(1, set.size());
    }

}
