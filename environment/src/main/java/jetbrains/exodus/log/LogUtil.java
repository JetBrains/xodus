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
package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FilenameFilter;

@SuppressWarnings("WeakerAccess")
public final class LogUtil {

    public static final int LOG_BLOCK_ALIGNMENT = 1024; // log files are aligned by kilobytes
    public static final int LOG_FILE_NAME_LENGTH = 11;
    public static final int LOG_FILE_EXTENSION_LENGTH = 3;
    public static final int LOG_FILE_NAME_WITH_EXT_LENGTH = LOG_FILE_NAME_LENGTH + LOG_FILE_EXTENSION_LENGTH;
    public static final String LOG_FILE_EXTENSION = ".xd";
    private static final char[] LOG_FILE_EXTENSION_CHARS = LOG_FILE_EXTENSION.toCharArray();

    private static final FilenameFilter LOG_FILE_NAME_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.length() == LogUtil.LOG_FILE_NAME_WITH_EXT_LENGTH &&
                name.endsWith(LogUtil.LOG_FILE_EXTENSION);
        }
    };

    private static final char[] LOG_FILE_NAME_ALPHABET = "0123456789abcdefghijklmnopqrstuv".toCharArray();
    private static final IntHashMap<Integer> ALPHA_INDEXES;

    private LogUtil() {
    }

    static {
        ALPHA_INDEXES = new IntHashMap<>();
        final char[] alphabet = LOG_FILE_NAME_ALPHABET;
        for (int i = 0; i < alphabet.length; ++i) {
            ALPHA_INDEXES.put(alphabet[i], i);
        }
    }

    public static String getLogFilename(long address) {
        if (address < 0) {
            throw new ExodusException("Starting address of a log file is negative: " + address);
        }
        if (address % LOG_BLOCK_ALIGNMENT != 0) {
            throw new ExodusException("Starting address of a log file is badly aligned: " + address);
        }
        address /= LOG_BLOCK_ALIGNMENT;
        char[] name = new char[LOG_FILE_NAME_WITH_EXT_LENGTH];
        for (int i = 1; i <= LOG_FILE_NAME_LENGTH; i++) {
            name[LOG_FILE_NAME_LENGTH - i] = LOG_FILE_NAME_ALPHABET[(int) (address & 0x1f)];
            address >>= 5;
        }
        System.arraycopy(LOG_FILE_EXTENSION_CHARS, 0, name, LOG_FILE_NAME_LENGTH, LOG_FILE_EXTENSION_LENGTH);
        return new String(name);
    }

    public static long getAddress(final String logFilename) {
        final int length = logFilename.length();
        if (length != LOG_FILE_NAME_WITH_EXT_LENGTH || !logFilename.endsWith(LOG_FILE_EXTENSION)) {
            throw new ExodusException("Invalid log file name: " + logFilename);
        }
        long address = 0;
        for (int i = 0; i < LOG_FILE_NAME_LENGTH; ++i) {
            final char c = logFilename.charAt(i);
            final Integer integer = ALPHA_INDEXES.get(c);
            if (integer == null) {
                throw new ExodusException("Invalid log file name: " + logFilename);
            }
            address = (address << 5) + integer;
        }
        return address * LOG_BLOCK_ALIGNMENT;
    }

    @SuppressWarnings("unused")
    public static boolean isLogFile(@NotNull final File file) {
        return isLogFileName(file.getName());
    }

    public static boolean isLogFileName(@NotNull final String name) {
        try {
            getAddress(name);
            return true;
        } catch (ExodusException e) {
            return false;
        }
    }

    @NotNull
    public static File[] listFiles(@NotNull final File directory) {
        return IOUtil.listFiles(directory, LOG_FILE_NAME_FILTER);
    }

    public static String getWrongAddressErrorMessage(final long address, final long fileSize /* in Kb */) {
        final long fileAddress = address - (address % (fileSize * 1024L));
        return ", address = " + address + ", file = " + getLogFilename(fileAddress);
    }
}
