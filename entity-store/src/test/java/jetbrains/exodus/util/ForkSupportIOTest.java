/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class ForkSupportIOTest {
    private static final Log log = LogFactory.getLog(ForkSupportIOTest.class);

    @Test
    public void testIO() throws Exception {
        final ForkSupportIO forked = ForkSupportIO.create(FL.class, new String[]{}, new String[]{"hello"}).start();
        log.info("Parent receiving...");
        Assert.assertEquals("hello", forked.readString());
        log.info("Parent sending...");
        forked.writeString("buzz off");
        Assert.assertEquals(0, forked.waitFor());
        forked.close();
    }

    public static class FL extends ForkedLogic {
        @Override
        public void forked(String[] args) throws Exception {
            log.info("I'm up!");

            log.info("Child sending...");
            getStreamer().writeString(args[0]);

            log.info("Child receiving...");
            Assert.assertEquals("buzz off", getStreamer().readString());

            close();
            log.info("Party over.");
        }
    }
}
