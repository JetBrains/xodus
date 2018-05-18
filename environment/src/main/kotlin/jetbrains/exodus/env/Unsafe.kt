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
package jetbrains.exodus.env

import jetbrains.exodus.ExodusException
import jetbrains.exodus.io.Block
import jetbrains.exodus.log.*
import kotlin.concurrent.withLock

fun <T> EnvironmentImpl.executeInCommitLock(action: () -> T): T {
    return synchronized(commitLock) {
        action()
    }
}

fun <T> EnvironmentImpl.executeInMetaWriteLock(action: () -> T): T {
    return metaWriteLock.withLock {
        action()
    }
}

fun EnvironmentImpl.reopenMetaTree(proto: MetaTreePrototype, rollbackTo: LogTip, confirm: () -> LogTip) {
    var logTip: LogTip? = null
    try {
        executeInMetaWriteLock {
            // confirm log inside of meta tree lock
            logTip = confirm().also {
                metaTreeInternal = MetaTreeImpl.create(this, it, proto)
            }
        }
    } catch (t: Throwable) {
        logTip?.let {
            // if log is confirmed, but metaTree can't be reopened, rollback log
            log.setHighAddress(it, rollbackTo.highAddress)
        }
        throw ExodusException.toExodusException(t, "Failed to reopen MetaTree")
    }
}

internal fun EnvironmentImpl.tryUpdate(): Boolean {
    val tip = log.tip
    log.tryUpdate(tip)?.let {
        val updatedTip = it.second
        return executeInMetaWriteLock {
            try {
                log.compareAndSetTip(tip, updatedTip)
                val root = it.first.rootAddress
                loadMetaTree(root, updatedTip)?.let { metaTree ->
                    metaTreeInternal = MetaTreeImpl(metaTree, root, updatedTip).also {
                        MetaTreeImpl.cloneTree(metaTree) // try to traverse meta tree
                    }
                    true
                } ?: run {
                    // Throwable class is used for exceptions to prevent stacktrace masking
                    throwableOnCommit = Throwable("Cannot load updated meta tree")
                    false
                }
            } catch (t: Throwable) {
                throwableOnCommit = Throwable("Cannot read updated meta tree", t)
                true
            }
        }
    }
    return false
}

// advance to some root loggable
internal fun Log.tryUpdate(tip: LogTip): Pair<DatabaseRoot, LogTip>? {
    val fileSet = tip.fileSetCopy
    val addedBlocks = config.reader.getBlocks(getFileAddress(tip.highAddress))
    val itr = addedBlocks.iterator()
    if (!itr.hasNext()) {
        return null
    }
    val lastBlock: Block
    val highAddress: Long
    while (true) {
        val block = itr.next()
        val blockAddress = block.address
        fileSet.add(blockAddress)
        if (!itr.hasNext()) {
            highAddress = blockAddress + block.length()
            lastBlock = block
            break
        }
    }
    // create loggable
    // update "last page"
    val lastBlockAddress = lastBlock.address
    val startAddress = maxOf(tip.highAddress, lastBlockAddress)
    if (startAddress > lastBlockAddress + fileLengthBound) {
        throw IllegalStateException("Log truncated abnormally, aborting")
    }
    val dataIterator = BlockDataIterator(this, tip, lastBlock, startAddress)
    val loggables = LoggableIteratorUnsafe(this, dataIterator)
    val type = DatabaseRoot.DATABASE_ROOT_TYPE
    var lastRoot: DatabaseRoot? = null
    while (loggables.hasNext()) {
        val loggable = loggables.next()
        val loggableEnd = loggable.address + loggable.length()
        if (loggableEnd > highAddress) {
            break
        }
        if (loggable.type == type) {
            lastRoot = DatabaseRoot(loggable, loggables.iterator)
        } else if (!NullLoggable.isNullLoggable(loggable)) {
            // don't skip DatabaseRoot content
            val expectedDataLength = loggable.dataLength.toLong()
            if (loggables.iterator.skip(expectedDataLength) < expectedDataLength) {
                return null
            }
        }
        if (loggableEnd != dataIterator.address) {
            return null
        }
        if (loggableEnd == highAddress) {
            break
        }
    }
    if (lastRoot == null) {
        return null
    }
    return lastRoot to with(dataIterator) {
        LogTip(lastPage, lastPageAddress, lastPageCount, highAddress, highAddress, fileSet.endWrite())
    }
}

internal class LoggableIteratorUnsafe(private val log: Log, internal val iterator: ByteIteratorWithAddress) : Iterator<RandomAccessLoggable> {

    fun getHighAddress() = iterator.address

    override fun next(): RandomAccessLoggable {
        if (!hasNext()) {
            throw IllegalStateException()
        }
        return log.read(iterator)
    }

    override fun hasNext() = iterator.hasNext()
}
