/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.entitystore.util.EntityIdSet;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.util.ByteArraySizedInputStream;
import jetbrains.exodus.util.LightByteArrayOutputStream;
import jetbrains.exodus.util.UTFUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.Date;

public class SshdTests extends EntityStoreTestBase {

    private int port;

    private int findFreePort() {
        try {
            ServerSocket s = new ServerSocket(0);
            int p = s.getLocalPort();
            s.close();

            return p;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected PersistentEntityStoreImpl createStoreInternal(String dbTempFolder) throws Exception {
        final PersistentEntityStoreConfig config = new PersistentEntityStoreConfig();
        config.setSshdPassword("epta");
        config.setSshdPort(port = findFreePort());
        return PersistentEntityStores.newInstance(config, Environments.newInstance(dbTempFolder, new EnvironmentConfig()), null, "persistentEntityStore");
    }

    public void testStart() throws Exception {
        ping();
    }

    public void testStop() throws Exception {
        tearDown();
        isPartiallyTornDown = true;
        try {
            ping();
        } catch (IOException e) {
            // ok
            return;
        }
        // fail!
        fail("Ping ok means sshd server didn't stop after db close");
    }

    private void ping() throws IOException {
        Socket s = new Socket("localhost", port);
        BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
        System.out.println(br.readLine());
    }

}
