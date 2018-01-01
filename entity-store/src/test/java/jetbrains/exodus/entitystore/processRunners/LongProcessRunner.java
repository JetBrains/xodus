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
package jetbrains.exodus.entitystore.processRunners;

import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongProcessRunner extends ProcessRunner {

    private static final Logger logger = LoggerFactory.getLogger(LongProcessRunner.class);

    @Override
    protected void oneMoreStep() throws Exception {
        logger.info("One more step...");
        EntityIterable iterable = txn.find("Person", "name", "Vadim");
        Entity entity = iterable.iterator().next();

        logger.info("Starting separate thread...");
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5 * 1000);
                    logger.info("Requesting euthanasia");
                    getStreamer().writeString(store.getLocation());
                } catch (Exception e) {
                    logger.error("Something went terribly wrong: ", e);
                }
            }
        }.start();
        logger.info("...separate thread started");

        logger.info("Getting ready for a rush...");
        long l = 0;
        while (true) {
            entity.setProperty("password", "dummypassword" + l++);
            Thread.sleep(0);
        }
    }
}
