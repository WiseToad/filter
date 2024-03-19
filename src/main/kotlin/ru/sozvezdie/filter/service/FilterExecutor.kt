package ru.sozvezdie.filter.service

import org.springframework.stereotype.Service
import ru.sozvezdie.filter.filter.AbstractFilter
import java.util.concurrent.Executors

@Service
class FilterExecutor {

    private val executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("filter-", 1).factory())

    fun execute(filter: AbstractFilter) {
        executor.execute(filter)
    }
}
