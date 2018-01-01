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

import jetbrains.exodus.ByteIterable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

@SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod"})
public class BTreeStructureTest extends BTreeTestBase {
    public BTreeBalancePolicy policy = new BTreeBalancePolicy(10);
    static int val = 0;

    BTreeMutable add(String a) {
        tm.add(key(a), value("v " + val++));
        return (BTreeMutable) tm;
    }

    ByteIterable get(String a) {
        return tm.get(key(a));
    }

    BTreeMutable refresh() {
        long a = tm.save();
        t = new BTree(log, policy, a, false, 1);
        tm = getTree().getMutableCopy();
        return (BTreeMutable) tm;
    }


    @Test
    public void simple() throws IOException {
        tm = new BTreeEmpty(log, policy, false, 1).getMutableCopy();
        add("c");
        for (int i = 0; i <= 6; i++) {
            add("a" + i);
        }
        refresh();
        for (int i = 0; i <= 2; i++) {
            add("e" + i);
        }
        add("d9");
        add("d8");
        Assert.assertNotNull(get("c"));
    }

    @Test
    public void childExistsTest() throws IOException {
        tm = new BTreeEmpty(log, policy, false, 1).getMutableCopy();
        for (int i = 0; i <= 60; i++) {
            tm.add(key("k " + i), value("v " + i));
        }
        refresh();
        BasePage root = getTreeMutable().getRoot();
        InternalPage child = (InternalPage) root.getChild(0);
        Assert.assertTrue(root.childExists(child.getKey(0).getKey(), child.getChildAddress(0)));
    }

}
