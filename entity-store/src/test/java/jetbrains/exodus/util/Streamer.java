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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

public class Streamer implements IStreamer {

    private static final Logger logger = LoggerFactory.getLogger(ForkSupportIOTest.class);

    private static final int BUFFER_SIZE = 1024;

    @NotNull
    private final BufferedReader socketInput;
    @NotNull
    private final BufferedWriter socketOutput;

    public Streamer(@NotNull Socket socket) throws IOException {
        socketInput = new BufferedReader(new InputStreamReader(socket.getInputStream()), BUFFER_SIZE);
        socketOutput = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public Streamer(@NotNull InputStream socketInput, @NotNull OutputStream socketOutput) {
        this.socketInput = new BufferedReader(new InputStreamReader(socketInput), BUFFER_SIZE);
        this.socketOutput = new BufferedWriter(new OutputStreamWriter(socketOutput));
    }

    @Override
    @Nullable
    public String readString() {
        try {
            return socketInput.readLine();
        } catch (IOException ioe) {
            logger.warn("Can't read string from input", ioe);
            return null;
        }
    }

    @Override
    public void writeString(@NotNull String data) throws IOException {
        socketOutput.write(data + '\n');
        socketOutput.flush();
    }

    @Override
    public void close() throws IOException {
        socketInput.close();
        socketOutput.close();
    }
}
