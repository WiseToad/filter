package ru.sozvezdie.filter.common

fun sleep(millis: Long): Boolean {
    try {
        Thread.sleep(millis)
    } catch (e: InterruptedException) {
        return false
    }
    return true
}
