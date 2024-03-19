package ru.sozvezdie.filter.common

// class to use whenever comparison is needed (including implicit ones in Sets and/or Maps)
class BinaryToken(
    val bytes: ByteArray
){
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return bytes.contentEquals((other as BinaryToken).bytes)
    }

    override fun hashCode(): Int = bytes.contentHashCode()

    override fun toString(): String = bytes.contentToString()
}

fun ByteArray.toBinaryToken(): BinaryToken = BinaryToken(this)
fun ByteBuilder.toBinaryToken(): BinaryToken = BinaryToken(build())
