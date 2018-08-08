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
