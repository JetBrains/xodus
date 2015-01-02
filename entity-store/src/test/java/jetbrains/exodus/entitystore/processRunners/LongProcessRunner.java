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
package jetbrains.exodus.entitystore.processRunners;

import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityIterable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LongProcessRunner extends ProcessRunner {

    private static final Log log = LogFactory.getLog(LongProcessRunner.class);

    @Override
    protected void oneMoreStep() throws Exception {
        log.info("One more step...");
        EntityIterable iterable = txn.find("Person", "name", "Vadim");
        Entity entity = iterable.iterator().next();

        log.info("Starting separate thread...");
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5 * 1000);
                    log.info("Requesting euthanasia");
                    getStreamer().writeString(store.getLocation());
                } catch (Exception e) {
                    log.error("Something went terribly wrong: ", e);
                }
            }
        }.start();
        log.info("...separate thread started");

        log.info("Getting ready for a rush...");
        long l = 0;
        while (true) {
            entity.setProperty("password", "dummypassword" + l++);
            Thread.sleep(0);
        }
    }
}
