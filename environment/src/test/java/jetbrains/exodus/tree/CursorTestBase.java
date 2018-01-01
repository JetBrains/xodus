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
package jetbrains.exodus.tree;

import jetbrains.exodus.env.Cursor;

import static org.junit.Assert.assertEquals;

public abstract class CursorTestBase extends TreeBaseTest {
    public final int s = 1000;

    protected void check(ITree tm, GetNext r) {
        Cursor c = tm.openCursor();
        assertEquals(false, c.getKey().iterator().hasNext());
        assertEquals(false, c.getValue().iterator().hasNext());

        for (int i = 0; i < s; i++) {
            assertEquals(true, r.n(c));
            assertEquals(c.getValue(), value("v" + i));
            assertEquals(c.getKey(), key(i));
        }

        assertEquals(false, r.n(c));
    }

    protected void check(ITree tm, GetPrev r) {
        Cursor c = tm.openCursor();
        for (int i = 0; i < s; i++) {
            assertEquals(true, r.p(c));
            assertEquals(c.getValue(), value("v" + (s - i - 1)));
            assertEquals(c.getKey(), key(s - i - 1));
        }
        assertEquals(false, r.p(c));
        c.close();
    }

    protected interface GetNext {
        boolean n(Cursor c);
    }

    protected interface GetPrev {
        boolean p(Cursor c);
    }
}
