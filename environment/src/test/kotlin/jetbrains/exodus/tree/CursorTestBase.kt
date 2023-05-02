/*
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
package jetbrains.exodus.tree

import jetbrains.exodus.env.Cursor
import org.junit.Assert

abstract class CursorTestBase : TreeBaseTest<ITree, ITreeMutable>() {
    val s = 1000
    fun check(tm: ITree, r: GetNext) {
        val c: Cursor = tm.openCursor()
        Assert.assertFalse(c.key.iterator().hasNext())
        Assert.assertFalse(c.value.iterator().hasNext())
        for (i in 0 until s) {
            Assert.assertTrue(r.n(c))
            Assert.assertEquals(c.value, value("v$i"))
            Assert.assertEquals(c.key, key(i))
        }
        Assert.assertFalse(r.n(c))
    }

    fun check(tm: ITree, r: GetPrev) {
        val c: Cursor = tm.openCursor()
        for (i in 0 until s) {
            Assert.assertTrue(r.p(c))
            Assert.assertEquals(c.value, value("v" + (s - i - 1)))
            Assert.assertEquals(c.key, key(s - i - 1))
        }
        Assert.assertFalse(r.p(c))
        c.close()
    }

   interface GetNext {
        fun n(c: Cursor?): Boolean
    }

    interface GetPrev {
        fun p(c: Cursor?): Boolean
    }
}
