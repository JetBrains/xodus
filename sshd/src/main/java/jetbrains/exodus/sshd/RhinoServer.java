package jetbrains.exodus.sshd;

import jetbrains.exodus.entitystore.PersistentEntityStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sshd.SshServer;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class RhinoServer {

    private SshServer sshd;
    private static final Log log = LogFactory.getLog(RhinoServer.class);

    public RhinoServer(int port, @Nullable String password, @NotNull PersistentEntityStore entityStore) throws IOException {
        log.info("Run sshd server on port [" + port + "] " + (password == null ? "with anonymous access" : "with password [" + password + "]") + " and database at [" + entityStore.getLocation() + "]");
        start(port, password, entityStore);
    }

    /**
     *
     * @param port
     * @param password null means any password will be accepted
     * @throws java.io.IOException
     */
    private void start(int port, @Nullable String password, @NotNull PersistentEntityStore entityStore) throws IOException {
        sshd = SshServer.setUpDefaultServer();
        sshd.setPort(port);
        File store = new File(System.getProperty("user.home"), ".xodus.cer");
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(store.getAbsolutePath()));
        sshd.setPasswordAuthenticator(new PasswordAuthenticatorImpl(password));
        sshd.setShellFactory(new RhinoCommandFactory(entityStore));

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
