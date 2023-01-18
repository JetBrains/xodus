/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;

public final class DirUtil {
    public static final int LUCENE_FILE_NAME_LENGTH = 13;
    public static final int LUCENE_FILE_EXTENSION_LENGTH = 4;

    public static final int LUCENE_FILE_NAME_WITH_EXT_LENGTH = LUCENE_FILE_NAME_LENGTH + LUCENE_FILE_EXTENSION_LENGTH;
    public static final String LUCENE_FILE_EXTENSION = ".lcd";

    private static final char[] LUCENE_FILE_NAME_ALPHABET = "0123456789abcdefghijklmnopqrstuv".toCharArray();

    private static final char[] LUCENE_FILE_EXTENSION_CHARS = LUCENE_FILE_EXTENSION.toCharArray();

    private static final IntHashMap<Integer> ALPHA_INDEXES;

    static {
        ALPHA_INDEXES = new IntHashMap<>();
        final char[] alphabet = LUCENE_FILE_NAME_ALPHABET;
        for (int i = 0; i < alphabet.length; ++i) {
            ALPHA_INDEXES.put(alphabet[i], Integer.valueOf(i));
        }
    }

    private static final Predicate<Path> LUCENE_FILE_FILTER = path -> {
        try {
            return Files.isRegularFile(path) && Files.size(path) == LUCENE_FILE_NAME_WITH_EXT_LENGTH && path.endsWith(LUCENE_FILE_EXTENSION);
        } catch (IOException e) {
            throw new ExodusException("Can not retrieve size of the file " + path, e);
        }
    };


    static @NotNull String getFileNameByAddress(long address) {
        if (address < 0) {
            throw new ExodusException("Starting address of a log file is negative: " + address);
        }
        char[] name = new char[LUCENE_FILE_NAME_WITH_EXT_LENGTH];

        for (int i = 1; i <= LUCENE_FILE_NAME_LENGTH; i++) {
            name[LUCENE_FILE_NAME_LENGTH - i] = LUCENE_FILE_NAME_ALPHABET[(int) (address & 0x1f)];
            address >>= 5;
        }

        System.arraycopy(LUCENE_FILE_EXTENSION_CHARS, 0, name, LUCENE_FILE_NAME_LENGTH, LUCENE_FILE_EXTENSION_LENGTH);
        return new String(name);
    }

    static long getFileAddress(String name) {
        final int length = name.length();
        if (length != LUCENE_FILE_NAME_WITH_EXT_LENGTH || !name.endsWith(LUCENE_FILE_EXTENSION)) {
            throw new ExodusException("Invalid log file name: " + name);
        }
        long address = 0;
        for (int i = 0; i < LUCENE_FILE_NAME_LENGTH; ++i) {
            final char c = name.charAt(i);
            final Integer integer = ALPHA_INDEXES.get(c);
            if (integer == null) {
                throw new ExodusException("Invalid log file name: " + name);
            }
            address = (address << 5) + integer.longValue();
        }
        return address;
    }

    static void readFully(@NotNull RandomAccessFile file, long offset, final byte[] page) throws IOException {
        int dataRead = 0;
        while (dataRead < page.length) {
            file.seek(offset + dataRead);
            int r = file.read(page, dataRead, page.length - dataRead);

            if (r == -1) {
                if (dataRead == 0) {
                    throw new EOFException("EOF for file " + file + " has been encountered");
                }

                return;
            }

            dataRead += r;
        }
    }

    static long getNextAddress(Path path) throws IOException {
        long[] maxAddressAndLength = new long[]{-1, -1};

        try (var fileStream = Files.walk(path)) {
            fileStream.filter(LUCENE_FILE_FILTER).forEach(p -> {
                        var address = getFileAddress(p.getFileName().toString());
                        if (address > maxAddressAndLength[0]) {
                            maxAddressAndLength[0] = address;
                            try {
                                maxAddressAndLength[1] = Files.size(p);
                            } catch (IOException e) {
                                throw new ExodusException("Error during fetching of size of file " + p, e);
                            }
                        }
                    }
            );
        }

        if (maxAddressAndLength[0] >= 0) {
            return maxAddressAndLength[0] + maxAddressAndLength[1];
        }

        return 0;
    }

}
