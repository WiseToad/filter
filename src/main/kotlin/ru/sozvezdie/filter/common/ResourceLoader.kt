package ru.sozvezdie.filter.common

import java.io.InputStream

object ResourceLoader {

    private class NoSuchResourceException(resourceName: String): RuntimeException("No such resource found: $resourceName")

    fun loadResourceAsString(resourceName: String): String =
        javaClass.getResource(resourceName)?.readText() ?: throw NoSuchResourceException(resourceName)

    fun getResourceAsStream(resourceName: String): InputStream =
        javaClass.classLoader.getResourceAsStream(resourceName) ?: throw NoSuchResourceException(resourceName)
}
