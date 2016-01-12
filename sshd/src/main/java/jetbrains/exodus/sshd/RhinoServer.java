/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.sshd;

import org.apache.sshd.SshServer;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class RhinoServer {

    private static final Logger log = LoggerFactory.getLogger(RhinoServer.class);

    private SshServer sshd;

    public RhinoServer(int port, @Nullable String password, @NotNull Map<String, Object> config) throws IOException {
        if (log.isInfoEnabled()) {
            log.info("Run sshd server on port [" + port + "] " + (password == null ? "with anonymous access" : "with password [" + password + "]"));
        }
        start(port, password, config);
    }

    /**
     *
     * @param port
     * @param password null means any password will be accepted
     * @throws java.io.IOException
     */
    private void start(int port, @Nullable String password, @NotNull Map<String, Object> config) throws IOException {
        sshd = SshServer.setUpDefaultServer();
        sshd.setPort(port);
        File store = new File(System.getProperty("user.home"), ".xodus.cer");
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(store.getAbsolutePath()));
        sshd.setPasswordAuthenticator(new PasswordAuthenticatorImpl(password));
        sshd.setShellFactory(new RhinoCommandFactory(config));

        sshd.start();
    }

    public void stop() throws InterruptedException {
        sshd.stop();
    }

    private static class PasswordAuthenticatorImpl implements PasswordAuthenticator {
        private String password;

        private PasswordAuthenticatorImpl(String password) {
            this.password = password;
        }

        @Override
        public boolean authenticate(String username, String password, ServerSession session) {
            return this.password == null || this.password.equals(password);
        }
    }
}
