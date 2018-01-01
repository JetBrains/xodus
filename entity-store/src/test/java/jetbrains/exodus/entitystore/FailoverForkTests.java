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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.entitystore.processRunners.ProcessRunner;
import jetbrains.exodus.util.ForkSupportIO;
import org.junit.Assert;

import java.io.InputStream;

@StoreArtifacts
public class FailoverForkTests extends EntityStoreTestBase {

    private ForkSupportIO forked;
    private String childFolderLocation;

    @Override
    protected void setUp() throws Exception {
        logger.info("Starting setup...");
        childFolderLocation = null;
        startAndKillProc();
        assertNotNull(childFolderLocation);
        super.setUp();
        logger.info("...Setup done");
    }

    @Override
    protected String getDatabaseFolder() {
        return childFolderLocation;
    }

    protected Class<? extends ProcessRunner> getProcessRunner() {
        return ProcessRunner.class;
    }

    private void startAndKillProc() throws Exception {
        logger.info("Preparing to start a process...");
        final String cipherId = System.getProperty("exodus.cipherId");
        final String[] jvmArgs = cipherId == null ? new String[0] :
            new String[]{"-Dexodus.cipherId=" + cipherId, "-Dexodus.cipherKey=" + System.getProperty("exodus.cipherKey"),
                "-Dexodus.cipherBasicIV=" + System.getProperty("exodus.cipherBasicIV")};
        forked = ForkSupportIO.create(getProcessRunner(), jvmArgs, new String[]{}).start();
        logger.info("Process started, PID = " + forked.getPID());
        childFolderLocation = forked.readString();
        logger.info("Forked process said: " + childFolderLocation);
        forked.kill();
        logger.info("Process killed");
        Assert.assertNotSame("Process ended normally", 0, forked.waitFor());
    }

    public void testDurability() throws Exception {
        EntityIterable persons = getStoreTransaction().find("Person", "name", "Vadim");
        Assert.assertEquals("Number of persons does not match after process crash", 1L, persons.size());
        Entity person = persons.iterator().next();
        Assert.assertEquals("Property value does not match", "dummypassword", person.getProperty("password"));
        try (InputStream weight = person.getBlob("weight")) {
            Assert.assertEquals("Blob LENGTH does not match", 1024 * 1024, weight.read(new byte[1024 * 1024]));
        }
        Assert.assertEquals("Some redundant properties are available in the entity", 2, person.getPropertyNames().size());
        Assert.assertEquals("Some redundant blobs are available in the entity", 1, person.getBlobNames().size());
    }

    /**
     * copied from EntityTest.testCreateSingleEntity. Sorry for copypasting, any ideas on how to reuse?
     */
    public void testCreateSingleEntity() {
        StoreTransaction txn = getStoreTransaction();
        Entity entity = txn.newEntity("Issue");
        txn.flush();
        Assert.assertNotNull(entity);
        Assert.assertTrue(entity.getId().getTypeId() >= 0);
        Assert.assertTrue(entity.getId().getLocalId() >= 0);
    }

    @Override
    protected void tearDown() throws Exception {
        forked.close();
        super.tearDown();
    }
}
