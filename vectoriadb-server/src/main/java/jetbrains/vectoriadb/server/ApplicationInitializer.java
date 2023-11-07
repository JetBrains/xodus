/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.server;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class ApplicationInitializer {
    private static final String CONFIG_NAME = "vectoriadb.conf";
    private static final Logger logger = LoggerFactory.getLogger(ApplicationInitializer.class);

    public static void main(String[] args) {
        var baseDir = System.getProperty(IndexManagerServiceImpl.BASE_PATH_PROPERTY);

        if (baseDir == null) {
            var msg = "Base path is not specified";

            logger.error(msg);
            throw new IllegalStateException(msg);
        }

        try {
            var basePath = Path.of(baseDir);
            var logsPath = basePath.resolve(IndexManagerServiceImpl.LOGS_DIR);
            var configPath = basePath.resolve(IndexManagerServiceImpl.CONFIG_DIR);
            var indexesPath = basePath.resolve(IndexManagerServiceImpl.INDEXES_DIR);

            Files.createDirectories(logsPath);
            Files.createDirectories(configPath);
            Files.createDirectories(indexesPath);

            logger.info("Initialization of VectoriaDB server started");
            logger.info("Base path: {}", baseDir);
            logger.info("Config path: {}", configPath);
            logger.info("Indexes path: {}", indexesPath);
            logger.info("Logs path: {}", logsPath);

            var copyConfig = false;

            InputStream configStream;
            if (Files.exists(configPath.resolve(CONFIG_NAME))) {
                configStream = Files.newInputStream(configPath.resolve(CONFIG_NAME));
            } else {
                configStream = ApplicationInitializer.class.getResourceAsStream("/" + CONFIG_NAME);
                logger.info("JVM config file {} does not exist. Will create one with default values",
                        configPath.resolve(CONFIG_NAME));
                copyConfig = true;
            }

            var configYaml = configPath.resolve(IndexManagerServiceImpl.CONFIG_YAML);
            if (!Files.exists(configYaml)) {
                logger.info("Server config file {} does not exist. Will create one with default values", configYaml);
                var defaultConfigStream = IndexManagerServiceImpl.class.getResourceAsStream("/" +
                        IndexManagerServiceImpl.CONFIG_YAML);

                assert defaultConfigStream != null;
                try (defaultConfigStream) {
                    Files.copy(defaultConfigStream, configYaml);
                }
            }

            if (configStream == null) {
                throw new IllegalStateException("JVM config file was not found");
            }

            var heapSizeSet = false;
            var directMemorySizeSet = false;

            var jvmParameters = new ArrayList<String>();
            try (var reader = new BufferedReader(new InputStreamReader(configStream))) {
                var line = reader.readLine().trim();
                while (line != null && !line.isEmpty()) {
                    if (!line.startsWith("#")) {
                        if (line.toLowerCase().startsWith("-xms") || line.toLowerCase().startsWith("-xmx")) {
                            heapSizeSet = true;
                        }
                        if (line.toLowerCase().startsWith("-xx:maxdirectmemorysize")) {
                            directMemorySizeSet = true;
                        }
                        jvmParameters.add(line);
                    }

                    line = reader.readLine();
                    if (line != null) {
                        line = line.trim();
                    }
                }
            }

            logger.info("JVM parameters: {}", String.join(" ", jvmParameters));

            if (copyConfig) {
                logger.info("Copying default JVM config file into the config directory : {}", configPath);

                try (var copyStream = ApplicationInitializer.class.getResourceAsStream("/" + CONFIG_NAME)) {
                    if (copyStream == null) {
                        throw new IllegalStateException("JVM config file is not found");
                    }

                    Files.copy(copyStream, configPath.resolve(CONFIG_NAME));
                }

                logger.info("Default JVM config file is copied");
            }


            var debugServerProperty = System.getProperty("vectoriadb.server.debug", "false");
            if (debugServerProperty.trim().isEmpty()) {
                debugServerProperty = "false";
            }
            var debugServer = Boolean.parseBoolean(debugServerProperty);

            var jvmCommandLine = new ArrayList<String>();
            jvmCommandLine.add("java");
            if (debugServer) {
                logger.info("Port 5005 is opened for remote debugging");
                jvmCommandLine.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005");
            }
            if (!directMemorySizeSet || !heapSizeSet) {
                var availableRam = IndexManagerServiceImpl.fetchAvailableRAM();
                logger.info("{} Mb of memory detected on server", availableRam / 1024 / 1024);

                if (!directMemorySizeSet) {
                    logger.info("Maximum amount of direct memory available for JVM set to {} Mb",
                            availableRam / 1024 / 1024);
                    jvmParameters.add("-XX:MaxDirectMemorySize=" + availableRam);
                }
                if (!heapSizeSet) {
                    logger.info("Maximum amount of heap memory size set to {} Mb", availableRam / 10 / 1024 / 1024);

                    jvmParameters.add("-Xmx" + availableRam / 10);
                    jvmParameters.add("-Xms" + availableRam / 10);

                }
            }

            jvmCommandLine.addAll(jvmParameters);
            jvmCommandLine.add("-D" + IndexManagerServiceImpl.BASE_PATH_PROPERTY + "=" + baseDir);
            jvmCommandLine.add("-cp");
            jvmCommandLine.add("/vectoriadb/bin/*");
            jvmCommandLine.add(VectoriaDBServer.class.getName());
            jvmCommandLine.add("--spring.config.location=file:" + configYaml.toAbsolutePath());

            logger.info("Starting VectoriaDB server with the following command line: {}",
                    String.join(" ", jvmCommandLine));
            var processBuilder = new ProcessBuilder(jvmCommandLine);
            processBuilder.inheritIO();
            var process = processBuilder.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Stopping VectoriaDB server");
                process.destroy();
            }, "VectoriaDB server shutdown hook"));

            process.waitFor();
            logger.info("VectoriaDB server is stopped");
        } catch (Throwable e) {
            logger.error("Initialization of VectoriaDB server failed", e);
            throw new RuntimeException(e);
        }
    }
}
