package ru.sozvezdie.filter.common

import java.nio.ByteBuffer
import java.time.LocalDate
import java.util.*

class ByteBuilder {

    companion object {
        const val COMPACTED_SHORT_PREFIX = 'W'.code.toByte()
        const val COMPACTED_INT_PREFIX = 'I'.code.toByte()
        const val COMPACTED_LONG_PREFIX = 'L'.code.toByte()

        const val COMPACTED_STRING_SHORT_PREFIX = 'w'.code.toByte()
        const val COMPACTED_STRING_INT_PREFIX = 'i'.code.toByte()
        const val COMPACTED_STRING_LONG_PREFIX = 'l'.code.toByte()
        const val COMPACTED_STRING_UUID_PREFIX = 'u'.code.toByte()
        const val COMPACTED_STRING_STRING_PREFIX = 's'.code.toByte()
    }

    private class Part(
        val size: Int,
        val addToBuffer: (ByteBuffer) -> Unit
    )

    private val parts = mutableListOf<Part>()

    fun add(value: Byte?): ByteBuilder = apply { if (value != null) addByte(value) }
    fun add(value: Short?): ByteBuilder = apply { if (value != null) addShort(value) }
    fun add(value: Int?): ByteBuilder = apply { if (value != null) addInt(value) }

    fun add(value: Long?, compacted: Boolean = true): ByteBuilder = apply {
        if (value != null) if (!compacted) addLong(value) else addCompactedLong(value)
    }

    // use compacted mode only if input string should represent numeric value or UUID
    fun add(value: String?, compacted: Boolean = false): ByteBuilder = apply {
        if (value != null) if (!compacted) addString(value) else addCompactedString(value)
    }

    fun add(value: ByteArray?): ByteBuilder = apply { if (value != null) addByteArray(value) }

    fun add(value: LocalDate?): ByteBuilder = apply { if (value != null) addByteArray(value.toByteArray()) }

    private fun addByte(value: Byte) {
        parts.add(Part(Byte.SIZE_BYTES) { it.put(value) })
    }

    private fun addShort(value: Short) {
        parts.add(Part(Short.SIZE_BYTES) { it.putShort(value) })
    }

    private fun addInt(value: Int) {
        parts.add(Part(Int.SIZE_BYTES) { it.putInt(value) })
    }

    private fun addLong(value: Long) {
        parts.add(Part(Long.SIZE_BYTES) { it.putLong(value) })
    }

    private fun addString(value: String) {
        addByteArray(value.toByteArray())
    }

    private fun addByteArray(value: ByteArray) {
        parts.add(Part(value.size) { it.put(value) })
    }

    private fun addCompactedLong(value: Long) {
        if (value <= Short.MAX_VALUE) {
            addByte(COMPACTED_SHORT_PREFIX)
            addShort(value.toShort())
        } else if (value <= Int.MAX_VALUE) {
            addByte(COMPACTED_INT_PREFIX)
            addInt(value.toInt())
        } else {
            addByte(COMPACTED_LONG_PREFIX)
            addLong(value)
        }
    }

    private fun addCompactedString(value: String) {
        val number = value.toLongOrNull()
        if (number != null) {
            if (number <= Short.MAX_VALUE) {
                addByte(COMPACTED_STRING_SHORT_PREFIX)
                addShort(number.toShort())
            } else if (number <= Int.MAX_VALUE) {
                addByte(COMPACTED_STRING_INT_PREFIX)
                addInt(number.toInt())
            } else {
                addByte(COMPACTED_STRING_LONG_PREFIX)
                addLong(number)
            }
        } else {
            try {
                val uuid = UUID.fromString(value)
                addByte(COMPACTED_STRING_UUID_PREFIX)
                addLong(uuid.mostSignificantBits)
                addLong(uuid.leastSignificantBits)
            }
            catch (e: IllegalArgumentException) {
                addByte(COMPACTED_STRING_STRING_PREFIX)
                addString(value)
            }
        }
    }

    fun build(): ByteArray {
        val buffer = ByteBuffer.allocate(parts.sumOf(Part::size))
        parts.forEach {
            it.addToBuffer(buffer)
        }
        return buffer.array()
    }
}
