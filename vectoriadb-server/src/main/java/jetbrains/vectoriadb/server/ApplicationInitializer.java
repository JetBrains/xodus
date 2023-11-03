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
            throw new IllegalStateException("Base path is not specified");
        }

        try {
            var basePath = Path.of(baseDir);
            var configPath = basePath.resolve(IndexManagerServiceImpl.CONFIG_DIR);
            var indexesPath = basePath.resolve(IndexManagerServiceImpl.INDEXES_DIR);
            var logsPath = basePath.resolve(IndexManagerServiceImpl.LOGS_DIR);

            Files.createDirectories(configPath);
            Files.createDirectories(indexesPath);
            Files.createDirectories(logsPath);

            logger.info("Initialization of vectoriadb server started");
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
                logger.error("Config file is not found in the base path, using default config");
                copyConfig = true;
            }

            var configYaml = configPath.resolve(IndexManagerServiceImpl.CONFIG_YAML);
            if (!Files.exists(configYaml)) {
                logger.info("Config file {} does not exist. Will create it with default values", configYaml);
                var defaultConfigStream = IndexManagerServiceImpl.class.getResourceAsStream("/" +
                        IndexManagerServiceImpl.CONFIG_YAML);

                assert defaultConfigStream != null;
                try (defaultConfigStream) {
                    Files.copy(defaultConfigStream, configYaml);
                }
            }

            if (configStream == null) {
                throw new IllegalStateException("Config file is not found");
            }

            var jvmParameters = new ArrayList<String>();
            try (var reader = new BufferedReader(new InputStreamReader(configStream))) {
                var line = reader.readLine().trim();
                while (line != null) {
                    if (!line.startsWith("#")) {
                        jvmParameters.add(line);
                    }

                    line = reader.readLine();
                }
            }

            logger.info("JVM parameters: {}", String.join(" ", jvmParameters));

            if (copyConfig) {
                logger.info("Copying default config into the config directory : {}", configPath);

                try (var copyStream = ApplicationInitializer.class.getResourceAsStream("/" + CONFIG_NAME)) {
                    if (copyStream == null) {
                        throw new IllegalStateException("Config file is not found");
                    }

                    Files.copy(copyStream, configPath.resolve(CONFIG_NAME));
                }

                logger.info("Default config is copied");
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
            jvmCommandLine.addAll(jvmParameters);
            jvmCommandLine.add("-D" + IndexManagerServiceImpl.BASE_PATH_PROPERTY + "=" + baseDir);
            jvmCommandLine.add("-cp");
            jvmCommandLine.add("/vectoriadb/bin/*");
            jvmCommandLine.add(VectoriaDBServer.class.getName());
            jvmCommandLine.add("--spring.config.location=file:" + configYaml.toAbsolutePath());

            logger.info("Starting vectoriadb server with the following command line: {}",
                    String.join(" ", jvmCommandLine));
            var processBuilder = new ProcessBuilder(jvmCommandLine);
            processBuilder.inheritIO();
            var process = processBuilder.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Stopping vectoriadb server");
                process.destroy();
            }, "VectoriaDB server shutdown hook"));

            process.waitFor();
            logger.info("VectoriaDB server is stopped");
        } catch (Throwable e) {
            logger.error("Initialization of vectoriadb server failed", e);
            throw new RuntimeException(e);
        }
    }
}
