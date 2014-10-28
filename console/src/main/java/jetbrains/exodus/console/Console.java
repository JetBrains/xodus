package jetbrains.exodus.console;

import org.apache.sshd.SshServer;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class Console {

    public static void main(String[] args) throws IOException {
        new Console().startServer();
    }

    private void startServer() throws IOException {
        SshServer sshd = SshServer.setUpDefaultServer();
        sshd.setPort(2222);
        File store = new File(System.getProperty("user.home"), ".xodus.cer");
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(store.getAbsolutePath()));
        sshd.setPasswordAuthenticator(new PasswordAuthenticatorImpl());
        sshd.setShellFactory(new RhinoCommandFactory());

        sshd.start();
    }

    private static class PasswordAuthenticatorImpl implements PasswordAuthenticator {
        @Override
        public boolean authenticate(String username, String password, ServerSession session) {
            return true;
        }
    }
}
