/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class TeamCityMessenger {

    private final PrintStream myPrintStream;

    public TeamCityMessenger(String fileName) throws IOException {
        myPrintStream = new PrintStream(new FileOutputStream(fileName, true));
    }

    public void putValue(String key, long value) {
        printValue(key, value, myPrintStream);
    }

    public void close() {
        myPrintStream.close();
    }

    public static void printValue(String key, long value, PrintStream myPrintStream) {
        myPrintStream.printf("##teamcity[buildStatisticValue key='%s' value='%d']%n", key, value);
    }
}
