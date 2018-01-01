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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@SuppressWarnings({"HardcodedFileSeparator", "UseOfProcessBuilder", "ObjectToString", "RawUseOfParameterizedType"})
public class ForkSupportIO implements IStreamer {

    private static final Logger logger = LoggerFactory.getLogger(ForkSupportIO.class);

    private static final int BUFFER_SIZE = 1024;

    private static final ProcessKiller[] PROCESS_KILLERS = {new WindowsProcessKiller()};

    private final ServerSocket serverSocket;
    private Streamer streamer;

    @NotNull
    private final String name;

    private Process process;
    private Thread err;
    private Thread out;
    private int processId;

    @NotNull
    private final ProcessBuilder builder; // well-suited to use as internal synchronization object

    protected ForkSupportIO(@NotNull final String name, @NotNull String[] jvmArgs, @NotNull String[] args) throws IOException {
        try {
            serverSocket = new ServerSocket(0, 10);
            logger.info("Listening on port: " + serverSocket.getLocalPort());
        } catch (IOException e) {
            logger.error("Failed to open server socket.", e);
            throw e;
        }
        this.name = name;

        // static does not suite here since System.getProperty result can vary
        final String javaHome = System.getProperty("java.home");
        if (javaHome == null || javaHome.length() == 0) {
            throw new IllegalStateException("java.home is undefined");
        }
        final File bin = new File(javaHome, "bin");
        File javaFile = new File(bin, "java");
        if (!(javaFile.exists() && javaFile.isFile())) {
            javaFile = new File(bin, "java.exe");
            if (!(javaFile.exists() && javaFile.isFile())) {
                throw new IllegalStateException(javaFile.getPath() + " doesn't exist");
            }
        }

        final String classpath = join(getClasspath(getClass()), File.pathSeparator);
        final String[] commonJvmArgs = {javaFile.getPath(),
                // "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=7777",
                "-cp", classpath
        };

        final List<String> trueArgs = new ArrayList<>();
        trueArgs.addAll(Arrays.asList(commonJvmArgs));
        trueArgs.addAll(Arrays.asList(jvmArgs));
        trueArgs.add(ForkedProcessRunner.class.getName());
        trueArgs.add(Integer.toString(serverSocket.getLocalPort()));
        trueArgs.add(name);
        trueArgs.addAll(Arrays.asList(args));

        logger.info("Ready to start process with arguments: " + trueArgs);
        builder = new ProcessBuilder(trueArgs);
    }

    public int getPID() {
        return processId;
    }

    @NotNull
    public static String join(@NotNull Collection<? extends String> strings, @NotNull final String separator) {
        final StringBuilder result = new StringBuilder();
        for (String string : strings) {
            if (string != null && !string.isEmpty()) {
                if (result.length() != 0) result.append(separator);
                result.append(string);
            }
        }
        return result.toString();
    }

    public static List<String> getClasspath(Class cls) {
        List<String> classpath = new ArrayList<>();
        URL[] urls = ((URLClassLoader) cls.getClassLoader()).getURLs();
        for (URL url : urls) {
            File f;
            try {
                f = new File(url.toURI());
            } catch (URISyntaxException e) {
                f = new File(url.getPath());
            }

            classpath.add(f.getAbsolutePath());
        }

        return classpath;
    }


    @NotNull
    private Process spawnProcess() throws IOException {
        final Process process = builder.start();
        logger.info("Waiting for connection...");
        final Socket connection = serverSocket.accept();
        logger.info("Connection received from " + connection.getInetAddress().getHostName());
        logger.info("Waiting to receive process Id");
        streamer = new Streamer(connection.getInputStream(), connection.getOutputStream());
        String idString = streamer.readString();
        processId = Integer.parseInt(idString);
        return process;
    }

    public ForkSupportIO start() throws IOException, InterruptedException {
        if (process == null) {
            if (logger.isInfoEnabled()) {
                logger.info("starting child process [" + name + ']');
                logger.info("============================================");
            }
            final Process spawned = spawnProcess();
            err = createSpinner(spawned.getErrorStream(), System.err, BUFFER_SIZE, "IO [err] " + name);
            out = createSpinner(spawned.getInputStream(), System.out, BUFFER_SIZE, "IO [out] " + name);
            process = spawned;
            return this;
        } else {
            throw new IllegalStateException("Process already started");
        }
    }

    private static Thread createSpinner(final InputStream input, final PrintStream output,
                                        final int bufferSize, final String title) {
        final Thread result = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final byte[] buf = new byte[bufferSize];

                    int i;
                    while ((i = input.read(buf, 0, bufferSize)) != -1) {
                        output.write(buf, 0, i);
                    }
                } catch (IOException ioe) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("IO error in child process for reader: " + input);
                    }
                }
            }
        }, title);
        result.start();
        return result;
    }

    public ForkSupportIO kill() throws InterruptedException {
        if (processId == -1) {
            process.destroy();
            return this;
        }
        for (ProcessKiller killer : PROCESS_KILLERS) {
            if (killer.suitableForOS(System.getProperty("os.name"))) {
                try {
                    killer.killProcess(processId);
                } catch (IOException e) {
                    logger.error("Failed to kill the process using dedicated killer. Will use process.destroy()", e);
                    process.destroy();
                }
                return this;
            }
        }
        logger.warn("Can not find dedicated killer for the process. Will use process.destroy()");
        process.destroy();
        return this;
    }

    public int waitFor() throws InterruptedException {
        final int status = process.waitFor();
        err.join();
        out.join();
        if (logger.isInfoEnabled()) {
            logger.info("============================================");
            logger.info("child process [" + name + "] finished: " + status);
        }
        process = null;
        out = err = null;
        return status;
    }

    @Override
    @Nullable
    public String readString() {
        return streamer.readString();
    }

    @Override
    public void writeString(@NotNull String data) throws IOException {
        streamer.writeString(data);
    }

    @Override
    public void close() throws IOException {
        try {
            streamer.close();
        } catch (IOException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Can't close streamer, ignoring", e);
            }
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Can't close socket, ignoring", e);
            }
        }
    }

    public static ForkSupportIO create(@NotNull final Class clazz, String[] jvmArgs, String[] args) throws IOException {
        return new ForkSupportIO(clazz.getName(), jvmArgs, args);
    }

    private static final FilenameFilter JAR_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".jar");
        }
    };

    private static final FilenameFilter CLASS_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".class");
        }
    };

    private static StringBuilder appendClassPath(final File directory, final StringBuilder builder) {
        for (final File dir : IOUtil.listFiles(directory)) {
            if (dir.isDirectory()) {
                if (dir.listFiles(JAR_FILTER).length == 0) {
                    if (builder.length() != 0) {
                        builder.append(File.pathSeparatorChar);
                    }
                    builder.append(dir.getPath());
                } else {
                    if (builder.length() != 0) {
                        builder.append(File.pathSeparatorChar);
                    }
                    builder.append(dir.getPath());
                    builder.append(File.separatorChar);
                    builder.append('*');
                    appendClassPath(dir, builder);
                }
            }
        }
        return builder;
    }
}
