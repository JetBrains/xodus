package jetbrains.exodus.log.replication

import jetbrains.exodus.log.Log
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.async.AsyncResponseHandler
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse

interface S3FactoryBoilerplate : FileFactory {
    val s3: S3AsyncClient
    val bucket: String
    val requestOverrideConfig: AwsRequestOverrideConfig?

    fun <T> getRemoteFile(length: Long, name: String, handler: AsyncResponseHandler<GetObjectResponse, T>): T {
        // if target log is appended in the meantime, ignore appended bytes thanks to S3 API Range header support
        // https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        return s3.getObject(
                GetObjectRequest.builder().range("bytes=0-${length - 1}")
                        .requestOverrideConfig(requestOverrideConfig).bucket(bucket).key(name).build(), handler
        ).get()
    }

    fun checkPreconditions(log: Log, expectedLength: Long): Boolean {
        if (expectedLength < 0L || expectedLength > log.fileLengthBound) {
            throw IllegalArgumentException("Incorrect expected length specified")
        }
        if (expectedLength == 0L) {
            return true
        }
        return false
    }
}
