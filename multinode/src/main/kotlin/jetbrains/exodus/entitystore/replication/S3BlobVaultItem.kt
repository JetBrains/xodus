/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.replication

import jetbrains.exodus.entitystore.BlobVaultItem
import software.amazon.awssdk.services.s3.model.S3Object


class S3BlobVaultItem(
        private val handle: Long,
        private val s3Object: S3Object
) : BlobVaultItem {

    override fun getHandle(): Long = handle

    override fun getLocation(): String = s3Object.key()

    override fun exists(): Boolean = true

    override fun toString(): String {
        return location
    }
}


class S3MissedBlobVaultItem(
        private val handle: Long,
        private val key: String
) : BlobVaultItem {

    override fun getHandle(): Long = handle

    override fun getLocation(): String = key

    override fun exists(): Boolean = false

    override fun toString(): String {
        return location
    }
}
