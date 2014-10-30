package jetbrains.exodus.console;

import jetbrains.exodus.entitystore.PersistentEntityStore;
import jetbrains.exodus.entitystore.PersistentEntityStores;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.sshd.RhinoServer;
import org.apache.commons.cli.*;

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

        new RhinoServer(port.intValue(), password, createEntityStore(path));
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

}
