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

import jetbrains.exodus.ExodusException;
import org.apache.lucene.util.IOFunction;
import org.apache.lucene.util.IORunnable;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

class DirectoryFileNamesRegistry {

    private final ConcurrentHashMap<String, Long> name2addr = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> addr2name = new ConcurrentHashMap<>();

    private final IORunnable betweenFileOperations;
    private final AtomicLong tempAddresses = new AtomicLong(-1);

    DirectoryFileNamesRegistry(IORunnable beforeFileOperations) {
        this.betweenFileOperations = beforeFileOperations;
    }

    public void register(String fileName, long address) throws IOException {
        materializeAddress(fileName, occupyName(fileName), address);
    }

    public long occupyName(String fileName) throws IOException {
        betweenFileOperations.run();
        final var tempAddr = tempAddresses.getAndDecrement();

        updateAddr2Name(tempAddr, prevName -> {
            if (prevName != null) {
                throw new IllegalStateException("Temporary address " + tempAddr + " is already taken, something went wrong.");
            }

            if (name2addr.putIfAbsent(fileName, tempAddr) != null) {
                throw new FileAlreadyExistsException(fileName);
            }

            betweenFileOperations.run();
            return fileName;
        });

        return tempAddr;
    }

    public void materializeAddress(String fileName, long tempAddress, long realAddress) throws IOException {
        betweenFileOperations.run();
        if (realAddress < 0) {
            throw new IllegalArgumentException("Address cannot be negative: " + realAddress);
        }
        if (tempAddress >= 0) {
            throw new IllegalArgumentException("Temporary address must be negative: " + tempAddress);
        }

        updateAddr2Name(realAddress, existingName -> {
            if (existingName != null) {
                throw new IllegalStateException("Address " + realAddress + " is already taken, something went wrong.");
            }
            name2addr.compute(fileName, (n, prevAddress) -> {
                if (prevAddress == null || prevAddress != tempAddress) {
                    throw new IllegalStateException("File name " + fileName + " doesn't have a temporary address associated with it, cannot materialize.");
                }

                return realAddress;
            });

            betweenFileOperations.run();
            return fileName;
        });

        addr2name.remove(tempAddress);
    }

    public long addressByName(String fileName) throws IOException {
        final var address = name2addr.get(fileName);
        if (address == null) {
            throw new NoSuchFileException("File " + fileName + " does not exist");
        }
        if (address < 0) {
            throw new IOException("Cannot access file " + fileName + " because it's currently being written.");
        }
        return address;
    }

    public String nameByAddress(long address) throws NoSuchFileException {
        if (address < 0) {
            throw new IllegalArgumentException("Address cannot be negative: " + address);
        }
        final var name = addr2name.get(address);
        if (name == null) {
            throw new NoSuchFileException("File with address " + address + " does not exist");
        }
        return name;
    }

    public long remove(String fileName) throws IOException {
        betweenFileOperations.run();
        final var address = addressByName(fileName);

        updateAddr2Name(address, existingName -> {
            betweenFileOperations.run();
            if (!Objects.equals(fileName, existingName)) {
                // has already been removed or renamed
                throw new NoSuchFileException(fileName);
            }

            if (name2addr.remove(fileName) == null) {
                throw new NoSuchFileException(fileName);
            }

            betweenFileOperations.run();
            return null;

        });

        return address;
    }

    public void rename(
            String sourceName,
            String destName,
            IORunnable fileMoveAction
    ) throws IOException {
        betweenFileOperations.run();
        if (Objects.equals(sourceName, destName)) {
            return;
        }
        final var address = addressByName(sourceName);

        updateAddr2Name(address, oldName -> {
            if (!Objects.equals(oldName, sourceName)) {
                // has already been removed or renamed
                throw new NoSuchFileException("File " + sourceName + " does not exist");
            }

            if (name2addr.putIfAbsent(destName, address) != null) {
                throw new FileAlreadyExistsException(destName);
            }
            betweenFileOperations.run();
            if (name2addr.remove(sourceName) == null) {
                // already removed in parallel thread
                name2addr.remove(destName, address);
                throw new NoSuchFileException("File " + destName + " does not exist");
            }

            betweenFileOperations.run();
            fileMoveAction.run();
            return destName;
        });
    }

    public Stream<String> fileNames() {
        return name2addr.entrySet()
                .stream()
                .filter(e -> e.getValue() >= 0)
                .map(Map.Entry::getKey);
    }

    private void updateAddr2Name(long address, IOFunction<String, String> action) throws IOException {
        try {
            addr2name.compute(address, (a, oldName) -> {
                try {
                    return action.apply(oldName);
                } catch (IOException e) {
                    throw new ExodusException(e);
                }
            });
        } catch (ExodusException e) {
            if (e.getCause() instanceof IOException) {
                throw ((IOException) e.getCause());
            }
            throw e;
        }
    }
}
