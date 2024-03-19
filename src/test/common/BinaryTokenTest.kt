package ru.sozvezdie.filter.common

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class BinaryTokenTest {

    @Test
    fun compare() {
        val a = BinaryToken(byteArrayOf(1, 2))
        val b = BinaryToken(byteArrayOf(1, 2))
        val c = BinaryToken(byteArrayOf(3, 4))

        assertTrue(a == b)
        assertTrue(a != c)
    }
}
