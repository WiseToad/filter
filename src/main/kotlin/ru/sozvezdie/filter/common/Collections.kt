package ru.sozvezdie.filter.common

fun stringListOf(vararg args: Any): List<String> = args.map(Any::toString)
fun stringArrayOf(vararg args: Any): Array<String> = stringListOf(*args).toTypedArray()

fun <T> Iterable<Pair<T, T>>.toInterleavedList(): List<T> =
    fold(mutableListOf()) { result, (key, value) ->
        result.apply { add(key); add(value) }
    }
