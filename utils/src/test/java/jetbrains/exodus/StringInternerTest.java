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
package jetbrains.exodus;

import jetbrains.exodus.util.StringInterner;
import org.junit.Assert;
import org.junit.Test;

public class StringInternerTest {

    @Test
    public void sameInstance() {
        final String firstString = "Intern me right away";
        //noinspection RedundantStringConstructorCall
        final String secondString = new String("Intern me") + " right away";
        Assert.assertEquals(firstString, secondString);
        Assert.assertNotSame(firstString, secondString);
        final String cached = StringInterner.intern(firstString);
        Assert.assertSame(cached, StringInterner.intern(secondString));
    }

    @Test
    public void substring() {
        final String firstString = "Intern me right away";
        //noinspection RedundantStringConstructorCall
        final String secondString = new String("Intern me") + " right away";
        Assert.assertEquals(firstString, secondString);
        Assert.assertNotSame(firstString, secondString);
        final String firstCached = StringInterner.intern(firstString.substring(0, 7));
        final String secondCached = StringInterner.intern(secondString.substring(0, 7));
        Assert.assertSame(firstCached, secondCached);
    }

    @Test
    public void customInterner() {
        final String firstString = "Intern me right away";
        //noinspection RedundantStringConstructorCall
        final String secondString = new String("Intern me") + " right away";
        Assert.assertNotSame(StringInterner.intern(firstString), StringInterner.newInterner(10).doIntern(secondString));
    }
}
