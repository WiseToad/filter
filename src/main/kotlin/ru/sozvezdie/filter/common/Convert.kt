package ru.sozvezdie.filter.common

import java.lang.IllegalArgumentException
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.*

private val epochStart = LocalDate.ofEpochDay(0)
const val DATE_SIZE_BYTES = Int.SIZE_BYTES

fun LocalDate.toInt(): Int = ChronoUnit.DAYS.between(epochStart, this).toInt()
fun Int.toLocalDate(): LocalDate = LocalDate.ofEpochDay(this.toLong())

fun LocalDate.toByteArray(): ByteArray = toInt().toByteArray()

fun Byte.toByteArray(): ByteArray = byteArrayOf(this)

fun Int.toByteArray(): ByteArray = ByteBuffer.allocate(Int.SIZE_BYTES).putInt(this).array()
fun ByteArray.toInt(): Int = ByteBuffer.wrap(this).getExactly(ByteBuffer::getInt)

fun Long.toByteArray(): ByteArray = ByteBuffer.allocate(Long.SIZE_BYTES).putLong(this).array()
fun ByteArray.toLong(): Long = ByteBuffer.wrap(this).getExactly(ByteBuffer::getLong)

fun ByteBuffer.getLocalDate() = getInt().toLocalDate()
fun ByteBuffer.getUUID() = UUID(getLong(), getLong())

fun ByteBuffer.getCompactedLong(): Long =
    when (get()) {
        ByteBuilder.COMPACTED_SHORT_PREFIX -> getShort().toLong()
        ByteBuilder.COMPACTED_INT_PREFIX -> getInt().toLong()
        ByteBuilder.COMPACTED_LONG_PREFIX -> getLong()
        else -> throw IllegalArgumentException("Unknown compaction prefix to extract value of Long type")
    }

fun ByteBuffer.getCompactedString(): String =
    when (get()) {
        ByteBuilder.COMPACTED_STRING_SHORT_PREFIX -> getShort().toString()
        ByteBuilder.COMPACTED_STRING_INT_PREFIX -> getInt().toString()
        ByteBuilder.COMPACTED_STRING_LONG_PREFIX -> getLong().toString()
        ByteBuilder.COMPACTED_STRING_UUID_PREFIX -> getUUID().toString()
        ByteBuilder.COMPACTED_STRING_STRING_PREFIX -> String(array(), arrayOffset() + position(), remaining()).also { position(limit()) }
        else -> throw IllegalArgumentException("Unknown compaction prefix to extract value of String type")
    }

fun ByteBuffer.checkNoDataLeft() {
    if (hasRemaining()) throw IllegalArgumentException("There is data left in the byte buffer after all operations have been done")
}

fun <T> ByteBuffer.getExactly(getValue: ByteBuffer.() -> T): T = getValue().also { checkNoDataLeft() }
