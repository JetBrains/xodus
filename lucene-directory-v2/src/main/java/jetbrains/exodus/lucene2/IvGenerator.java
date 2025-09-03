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

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class IvGenerator {

    public static int MAX_INCREMENT = 15;

    private final AtomicLong nextIv;
    private final Consumer<Long> ivGenListener;

    private final Random random = new Random();

    public IvGenerator(long startFrom, Consumer<Long> ivGenListener) {
        this.nextIv = new AtomicLong(startFrom + random.nextInt(MAX_INCREMENT));
        this.ivGenListener = ivGenListener;
    }

    public long generate() {
        final var iv = nextIv.getAndAdd(random.nextInt(MAX_INCREMENT) + 1);
        if (ivGenListener != null) {
            ivGenListener.accept(iv);
        }
        return iv;
    }

}
