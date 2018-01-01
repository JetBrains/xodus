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
package jetbrains.exodus.benchmark.persistit;

import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import com.persistit.logging.Slf4jAdapter;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

abstract class JMHPersistItTokyoCabinetBenchmarkBase {

    private static final ByteIterable[] successiveKeys = TokyoCabinetBenchmark.getSuccessiveEntries(TokyoCabinetBenchmark.KEYS_COUNT);
    static final ByteIterable[] randomKeys = TokyoCabinetBenchmark.getRandomEntries(TokyoCabinetBenchmark.KEYS_COUNT);

    private TemporaryFolder temporaryFolder;
    Persistit persistit;
    Volume volume;
    private Exchange exchange;

    public void setup() throws IOException, PersistitException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        createEnvironment();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException, PersistitException {
        closeDb();
    }

    void writeSuccessiveKeys(@NotNull final Exchange store) throws PersistitException {
        for (final ByteIterable key : successiveKeys) {
            exchange.clear();
            for (int i = 0; i < key.getLength(); i++) {
                exchange.append(key.getBytesUnsafe()[i]);
            }
            exchange.getValue().put(key.getBytesUnsafe());
            exchange.store();
        }
        persistit.flush();
    }

    Exchange createTestStore() throws PersistitException {
        if (exchange == null) {
            exchange = persistit.getExchange(volume, "testTokyoCabinet", true);
        }
        return exchange;
    }

    private void createEnvironment() throws IOException, PersistitException {
        closeDb();
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        persistit = new Persistit();
        persistit.setPersistitLogger(new Slf4jAdapter(LoggerFactory.getLogger("PERSISTIT")));
        Properties props = new Properties();
        props.setProperty("datapath", temporaryFolder.getRoot().getAbsolutePath());
        props.setProperty("logpath", "${datapath}/log");
        props.setProperty("logfile", "${logpath}/persistit_${timestamp}.log");
        props.setProperty("buffer.count.8192", "5000");
        props.setProperty("journalpath", "${datapath}/journal");
        props.setProperty("tmpvoldir", "${datapath}");
        props.setProperty("volume.1", "${datapath}/persistit,create,pageSize:8192,initialPages:10,extensionPages:100,maximumPages:25000");
        props.setProperty("jmx", "false");
        persistit.setProperties(props);
        persistit.initialize();
        volume = persistit.createTemporaryVolume();

    }

    private void closeDb() throws PersistitException {
        if (persistit != null) {
            persistit.releaseExchange(exchange);
            exchange = null;
            volume.close();
            volume.delete();
            volume = null;
            persistit.close(false);
            persistit = null;
        }
        if (temporaryFolder != null) {
            temporaryFolder.delete();
        }
    }
}
