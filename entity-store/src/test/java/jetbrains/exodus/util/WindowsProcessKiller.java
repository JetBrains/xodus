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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

@SuppressWarnings({"CallToRuntimeExec", "HardcodedFileSeparator"})
public class WindowsProcessKiller implements ProcessKiller {

    private static final IOException REGISTRATION_EXCEPTION;
    private static final String PS_KILL;

    static {
        IOException exception;
        String psKill;
        try {
            exception = null;
            if (check(System.getProperty("os.name"))) {
                Runtime.getRuntime().exec("reg add HKEY_CURRENT_USER\\Software\\Sysinternals\\PsKill /v EulaAccepted /t REG_DWORD /d 1 /f");
                final File psKillFile = new File(WindowsProcessKiller.class.getClassLoader().getResource("pskill.exe").getFile());
                if (!psKillFile.exists()) {
                    throw new FileNotFoundException("The pskill.exe resource file doesn't exist");
                }
                psKill = psKillFile.getAbsolutePath();
            } else {
                psKill = null;
            }
        } catch (final IOException e) {
            exception = e;
            psKill = null;
        }
        REGISTRATION_EXCEPTION = exception;
        PS_KILL = psKill;
    }

    @Override
    public void killProcess(int id) throws IOException {
        if (REGISTRATION_EXCEPTION != null) {
            // for some reason pskill registration was not successful. If we still try to run it here, it might hang
            throw REGISTRATION_EXCEPTION;
        }
        Runtime.getRuntime().exec(PS_KILL + id);
    }

    @Override
    public boolean suitableForOS(String osName) {
        return check(osName);
    }

    private static boolean check(String osName) {
        return osName.toLowerCase().contains("windows");
    }
}
