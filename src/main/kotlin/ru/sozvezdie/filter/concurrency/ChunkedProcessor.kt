package ru.sozvezdie.filter.concurrency

import ru.sozvezdie.filter.config.Constant
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

abstract class ChunkedProcessor<T>(
    pipeSize: Int,
    private val chunkSize: Int
): Runnable {

    lateinit var resultConsumer: Consumer<Collection<T>>

    private val dataPipe = LinkedBlockingQueue<T>(pipeSize)

    private val stopping = AtomicBoolean(false)
    private val stopped = Signal()

    @Volatile
    private var exception: Exception? = null

    override fun run() {
        try {
            val chunk = mutableListOf<T>()
            while (!Thread.interrupted() && !(stopping.get() && dataPipe.isEmpty())) {
                val element = dataPipe.poll(1, TimeUnit.SECONDS)
                if (element != null) {
                    chunk += element
                    dataPipe.drainTo(chunk, chunkSize - chunk.size)
                }
                if (chunk.size >= chunkSize) {
                    resultConsumer.accept(processChunk(chunk))
                    chunk.clear()
                }
            }
            if (chunk.isNotEmpty()) {
                resultConsumer.accept(processChunk(chunk))
            }
        }
        catch (e: Exception) {
            exception = e
        }
        finally {
            stopped.signalAll()
        }
    }

    protected abstract fun processChunk(elements: Collection<T>): Collection<T>

    fun putToPipe(element: T) {
        checkTimeout(!dataPipe.offer(element, Constant.CHUNKED_PROCESSOR_PUT_TIMEOUT.toLong(), TimeUnit.SECONDS))
    }

    fun finish() {
        stopping.set(true)
        checkTimeout(!stopped.await(Constant.CHUNKED_PROCESSOR_STOP_TIMEOUT.toLong(), TimeUnit.SECONDS))
    }

    private fun checkTimeout(isTimeout: Boolean) {
        if (exception != null) {
            throw RuntimeException("Thread terminated by exception", exception)
        }
        if (isTimeout) {
            throw RuntimeException("Operation timed out")
        }
    }
}
