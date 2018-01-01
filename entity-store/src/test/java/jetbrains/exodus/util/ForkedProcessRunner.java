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
package jetbrains.exodus.util;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Socket;
import java.util.Arrays;

public class ForkedProcessRunner {

    private static final Logger logger = LoggerFactory.getLogger(ForkSupportIO.class);

    public static final String FATAL_ERROR_MESSAGE = "This is a last goodbye. And here is some randomness: sdjwijernla";

    private static Socket socket = null;

    static Streamer streamer = null;

    private ForkedProcessRunner() {
    }

    @SuppressWarnings({"HardcodedFileSeparator"})
    public static void main(String[] args) throws Exception {
        logger.info("Process started. Arguments: " + Arrays.toString(args));
        if (args.length < 2) {
            exit("Arguments do not contain port number and/or class to be run. Exit.", null);
        }
        try {
            int port = Integer.parseInt(args[0]);
            socket = new Socket("localhost", port);
            streamer = new Streamer(socket);
        } catch (NumberFormatException e) {
            exit("Failed to parse port number: " + args[0] + ". Exit.", null);
        }
        ForkedLogic forkedLogic = null;
        try {
            Class<?> clazz = Class.forName(args[1]);
            forkedLogic = (ForkedLogic) clazz.getConstructor().newInstance();
        } catch (Exception e) {
            exit("Failed to instantiate or initialize ForkedLogic descendant", e);
        }
        // lets provide the peer with our process id
        streamer.writeString(getProcessId());
        String[] realArgs = new String[args.length - 2];
        System.arraycopy(args, 2, realArgs, 0, realArgs.length);
        forkedLogic.forked(realArgs);
    }

    @SuppressWarnings({"CallToSystemExit", "finally"})
    private static void exit(String logMessage, @Nullable Exception exc) {
        try {
            logger.error(logMessage, exc);
            if (streamer != null) {
                streamer.writeString(FATAL_ERROR_MESSAGE);
                streamer.close();
                socket.close();
            }
        } catch (IOException e) {
            logger.error("Error while closing streamer", e);
        } finally {
            System.exit(1);
        }
    }

    private static String getProcessId() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        String name = runtimeBean.getName();
        return name.substring(0, name.indexOf('@')); // yes, it's not documented, but name has form "PID@bullshit"
    }

    public static void close() {
        try {
            streamer.close();
        } catch (IOException e) {
            // nothing to do here
        }
        try {
            socket.close();
        } catch (IOException e) {
            // nothing to do here
        }
    }
}
