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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.TreeMetaInfo;
import org.jetbrains.annotations.NotNull;

public class PatriciaMetaInfo extends TreeMetaInfo {

    public PatriciaMetaInfo(@NotNull final Log log, final boolean duplicates, final int structureId) {
        super(log, duplicates, structureId);
    }

    @Override
    public boolean isKeyPrefixing() {
        return true;
    }

    @Override
    public TreeMetaInfo clone(final int newStructureId) {
        return new PatriciaMetaInfo(log, duplicates, newStructureId);
    }

    public static PatriciaMetaInfo load(@NotNull final EnvironmentImpl env, byte flagsByte, ByteIterator it) {
        final boolean duplicates = (flagsByte & DUPLICATES_BIT) != 0;
        CompressedUnsignedLongByteIterable.getInt(it); // legacy format
        final int structureId = CompressedUnsignedLongByteIterable.getInt(it);
        return new PatriciaMetaInfo(env.getLog(), duplicates, structureId);
    }
}
