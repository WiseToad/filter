package ru.sozvezdie.filter.concurrency

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class Signal {

    private val monitor = ReentrantLock()
    private val condition = monitor.newCondition()

    fun await() = doWithLock(condition::await)
    fun await(time: Long, unit: TimeUnit): Boolean = doWithLock { condition.await(time, unit) }
    fun signal() = doWithLock(condition::signal)
    fun signalAll() = doWithLock(condition::signalAll)

    private fun <R> doWithLock(action: () -> R): R {
        monitor.lock()
        try {
            return action()
        } finally {
            monitor.unlock()
        }
    }
}
