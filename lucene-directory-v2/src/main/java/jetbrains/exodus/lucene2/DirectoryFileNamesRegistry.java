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
package jetbrains.exodus.lucene2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

class DirectoryFileNamesRegistry {

    private final ConcurrentHashMap<String, Long> name2addr = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> addr2name = new ConcurrentHashMap<>();

    public void register(String fileName, long address) {
        if (address < 0) {
            throw new IllegalArgumentException("Address cannot be negative: " + address);
        }
        addr2name.put(address, fileName);
        name2addr.put(fileName, address);
    }

    public void prepareName(String fileName) throws FileAlreadyExistsException {
        if (name2addr.putIfAbsent(fileName, -1L) != null) {
            throw new FileAlreadyExistsException(fileName);
        }
    }

    public long addressByName(String fileName) throws IOException {
        final var address = name2addr.get(fileName);
        if (address == null) {
            throw new FileNotFoundException("File $fileName does not exist");
        }
        if (address < 0) {
            throw new IOException("File " + fileName + " cannot be read because it is being currently written.");
        }
        return address;
    }

    public String nameByAddress(long address) throws FileNotFoundException {
        if (address < 0) {
            throw new IllegalArgumentException("Address cannot be negative: " + address);
        }
        final var name = addr2name.get(address);
        if (name == null) {
            throw new FileNotFoundException("File with address $address does not exist");
        }
        return name;
    }

    public long remove(String fileName) throws FileNotFoundException {
        final var oldAddr = name2addr.remove(fileName);
        if (oldAddr == null) {
            throw new FileNotFoundException("File " + fileName + " does not exist");
        }
        addr2name.remove(oldAddr);
        return oldAddr;
    }

    public void rename(
            String sourceName,
            String destName,
            Runnable fileMoveAction
    ) throws IOException {
        final var sourceAddr = addressByName(sourceName);

        // this will allow new name to be instantly available
        name2addr.put(destName, sourceAddr);

        // making the old name unavailable for read
        name2addr.remove(sourceName);

        addr2name.compute(sourceAddr, (k, v) -> {
            fileMoveAction.run();
            return destName;
        });
    }

    public Stream<String> indexNames() {
        return name2addr.entrySet().stream()
                .filter(e -> e.getValue() >= 0)
                .map(Map.Entry::getKey);
    }
}
