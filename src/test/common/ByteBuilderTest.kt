package ru.sozvezdie.filter.common

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.LocalDate

class ByteBuilderTest {

    @Test
    fun buildFromLong() {
        val byteArray = ByteBuilder()
            .add(291L, false)
            .add(19088743L, false)
            .add(81985526925837671L, false)
            .build()

        val expected = byteArrayOf(
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x23,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x23, 0x45, 0x67,
            0x01, 0x23, 0x45, 0x67, 0x01, 0x23, 0x45, 0x67
        )

        Assertions.assertArrayEquals(expected, byteArray)
    }

    @Test
    fun buildFromLongCompacted() {
        val byteArray = ByteBuilder()
            .add(291L)
            .add(19088743L)
            .add(81985526925837671L)
            .build()

        val expected = byteArrayOf(
            ByteBuilder.COMPACTED_SHORT_PREFIX, 0x01, 0x23,
            ByteBuilder.COMPACTED_INT_PREFIX, 0x01, 0x23, 0x45, 0x67,
            ByteBuilder.COMPACTED_LONG_PREFIX, 0x01, 0x23, 0x45, 0x67, 0x01, 0x23, 0x45, 0x67
        )

        Assertions.assertArrayEquals(expected, byteArray)
    }

    @Test
    fun buildFromNumberStringCompacted() {
        val byteArray = ByteBuilder()
            .add("291", true)
            .add("19088743", true)
            .add("81985526925837671", true)
            .build()

        val expected = byteArrayOf(
            ByteBuilder.COMPACTED_STRING_SHORT_PREFIX, 0x01, 0x23,
            ByteBuilder.COMPACTED_STRING_INT_PREFIX, 0x01, 0x23, 0x45, 0x67,
            ByteBuilder.COMPACTED_STRING_LONG_PREFIX, 0x01, 0x23, 0x45, 0x67, 0x01, 0x23, 0x45, 0x67
        )

        Assertions.assertArrayEquals(expected, byteArray)
    }

    @Test
    fun buildFromUuidStringCompacted() {
        val byteArray = ByteBuilder()
            .add("01234567-89AB-CDEF-0123-456789abcdef", true)
            .build()

        val expected = byteArrayOf(
            ByteBuilder.COMPACTED_STRING_UUID_PREFIX,
            0x01, 0x23, 0x45, 0x67, 0x89.toByte(), 0xAB.toByte(), 0xCD.toByte(), 0xEF.toByte(),
            0x01, 0x23, 0x45, 0x67, 0x89.toByte(), 0xAB.toByte(), 0xCD.toByte(), 0xEF.toByte(),
        )

        Assertions.assertArrayEquals(expected, byteArray)
    }

    @Test
    fun buildFromStringCompacted() {
        val byteArray = ByteBuilder()
            .add("Hello, world!", true)
            .build()

        val expected = "sHello, world!".toByteArray()

        Assertions.assertArrayEquals(expected, byteArray)
    }

    @Test
    fun buildFromString() {
        val byteArray = ByteBuilder()
            .add("Hello, world!")
            .build()

        val expected = "Hello, world!".toByteArray()

        Assertions.assertArrayEquals(expected, byteArray)
    }

    @Test
    fun buildFromLocalDate() {
        val byteArray = ByteBuilder()
            .add(LocalDate.of(2023, 1, 1))
            .build()

        val expected = byteArrayOf(0, 0, 0x4B, 0x9E.toByte())

        Assertions.assertArrayEquals(expected, byteArray)
    }
}
