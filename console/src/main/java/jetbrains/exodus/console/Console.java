package jetbrains.exodus.console;

import jetbrains.exodus.entitystore.PersistentEntityStore;
import jetbrains.exodus.entitystore.PersistentEntityStores;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import org.apache.commons.cli.*;
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
public class Console {

    public static void main(String[] args) throws IOException, ParseException {
        CommandLine line = getCommandLine(args);

        Long port = (Long) line.getParsedOptionValue("l");
        port = port == null ? 2222 : port;
        String password = line.getOptionValue('p', null);
        String path = line.getOptionValue('x', System.getProperty("user.home") + File.separator + "teamsysdata");
        System.out.println("Run sshd server on port [" + port + "] " + (password == null ? "with anonymous access" : "with password [" + password + "]") + " and database at [" + path + "]");

        new Console().startServer(port.intValue(), password, createEntityStore(path));
    }

    private static PersistentEntityStore createEntityStore(String path) {
        return PersistentEntityStores.newInstance(Environments.newInstance(path, new EnvironmentConfig()), "teamsysstore");
    }

    private static CommandLine getCommandLine(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OptionBuilder.hasArg().withType(Number.class).withDescription("sshd port").create('l'));
        options.addOption(OptionBuilder.hasArg().withDescription("password to login").create('p'));
        options.addOption(OptionBuilder.hasArg().withDescription("xodus database path").create('x'));

        if (args.length <= 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(Console.class.getCanonicalName(), options);
        }

        // create the parser
        CommandLineParser parser = new BasicParser();
        return parser.parse(options, args);
    }

    /**
     *
     * @param port
     * @param password null means any password will be accepted
     * @throws IOException
     */
    public void startServer(int port, @Nullable String password, @NotNull PersistentEntityStore entityStore) throws IOException {
        SshServer sshd = SshServer.setUpDefaultServer();
        sshd.setPort(port);
        File store = new File(System.getProperty("user.home"), ".xodus.cer");
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(store.getAbsolutePath()));
        sshd.setPasswordAuthenticator(new PasswordAuthenticatorImpl(password));
        sshd.setShellFactory(new RhinoCommandFactory(entityStore));

        sshd.start();
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
