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
package jetbrains.exodus.console;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.sshd.RhinoServer;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class Console {

    public static void main(String[] args) throws IOException, ParseException {
        CommandLine line = getCommandLine(args);

        Long port = (Long) line.getParsedOptionValue("l");
        port = port == null ? 2222 : port;
        String password = line.getOptionValue('p', null);

        Map<String, Object> config = new HashMap<>();
        config.put("location", line.getOptionValue('x', null));

        new RhinoServer(port.intValue(), password, config);
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
