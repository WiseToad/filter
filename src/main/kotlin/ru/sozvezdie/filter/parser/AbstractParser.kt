package ru.sozvezdie.filter.parser

import ru.sozvezdie.filter.domain.Element
import java.io.InputStream
import java.lang.RuntimeException
import java.util.function.Consumer

abstract class AbstractParser {

    lateinit var resultConsumerResolver: (Class<out Element>) -> Consumer<Element>?

    abstract fun parse(inputStream: InputStream)

    protected inline fun <reified E: Element> sendResult(element: E) {
        val consumer = resultConsumerResolver(element::class.java) ?: throw RuntimeException("Unresolved element type for parser: ${element::class.qualifiedName}")
        consumer.accept(element)
    }
}
