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
package jetbrains.exodus.io

import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.env.EnvironmentImpl

class WatchingFileDataReaderWriterProvider : DataReaderWriterProvider() {

    var env: EnvironmentImpl? = null

    override fun isReadonly() = true

    override fun newReaderWriter(location: String): Pair<DataReader, DataWriter> {
        val pair = FileDataReaderWriterProvider().newReaderWriter(location)
        return Pair(WatchingFileDataReader({ env }, pair.first as FileDataReader), pair.second)
    }
}
