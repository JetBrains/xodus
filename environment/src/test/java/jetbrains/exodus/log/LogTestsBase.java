/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.io.*;
import jetbrains.exodus.util.IOUtil;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

class LogTestsBase {

    protected volatile Log log;
    private File logDirectory = null;
    protected DataReader reader;
    protected DataWriter writer;

    @Before
    public void setUp() throws IOException {
        SharedOpenFilesCache.setSize(16);
        final File testsDirectory = getLogDirectory();
        if (testsDirectory.exists()) {
            IOUtil.deleteRecursively(testsDirectory);
        } else if (!testsDirectory.mkdir()) {
            throw new IOException("Failed to create directory for tests.");
        }

        synchronized (this) {
            final Pair<DataReader, DataWriter> logRW = createLogRW();
            reader = logRW.getFirst();
            writer = logRW.getSecond();
        }
    }

    @After
    public void tearDown() {
        closeLog();
        final File testsDirectory = getLogDirectory();
        IOUtil.deleteRecursively(testsDirectory);
        IOUtil.deleteFile(testsDirectory);
        logDirectory = null;
    }

    protected Pair<DataReader, DataWriter> createLogRW() {
        FileDataReader reader = new FileDataReader(logDirectory);
        return new Pair<>(reader, new FileDataWriter(reader));
    }

    void initLog(final long fileSize, final int cachePageSize) {
        initLog(new LogConfig().setFileSize(fileSize).setCachePageSize(cachePageSize));
    }

    void initLog(final LogConfig config) {
        if (log == null) {
            synchronized (this) {
                if (log == null) {
                    log = new Log(config.setReaderWriter(reader, writer));
                }
            }
        }
    }

    protected Log getLog() {
        if (log == null) {
            synchronized (this) {
                if (log == null) {
                    log = new Log(LogConfig.create(reader, writer));
                }
            }
        }
        return log;
    }

    void closeLog() {
        if (log != null) {
            log.close();
            log = null;
        }
    }

    File getLogDirectory() {
        if (logDirectory == null) {
            logDirectory = TestUtil.createTempDir();
        }
        return logDirectory;
    }

    static TestLoggable createOneKbLoggable() {
        return new TestLoggable((byte) 126, new ArrayByteIterable(new byte[1024], 1024), Loggable.NO_STRUCTURE_ID);
    }

}
