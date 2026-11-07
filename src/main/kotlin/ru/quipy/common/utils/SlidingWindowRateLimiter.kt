package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.ceil

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    companion object {
        const val WINDOW_MILLIS = 1010.0

        fun intervalNanos(ratePerSecond: Double): Long {
            require(ratePerSecond > 0 && ratePerSecond.isFinite())
            return ceil(WINDOW_MILLIS * 1_000_000 / ratePerSecond).toLong()
        }
    }

    init {
        require(rate > 0)
        require(!window.isNegative && !window.isZero)
    }

    private val windowNanos = window.toNanos()
    private val lock = ReentrantLock(true)
    private val dispatches = ArrayDeque<Long>()

    override fun tick(): Boolean {
        if (!lock.tryLock()) return false
        try {
            return tryAcquire(System.nanoTime())
        } finally {
            lock.unlock()
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    fun awaitPermission(deadlineAt: Long): Boolean {
        while (true) {
            val nowMillis = System.currentTimeMillis()
            val remainingMillis = deadlineAt - nowMillis
            if (remainingMillis <= 0) return false

            // Блокировка захватывается ТОЛЬКО для быстрых операций с очереди
            val waitNanos = lock.withLock {
                val now = System.nanoTime()
                if (tryAcquire(now)) {
                    return true
                }
                val oldest = dispatches.peekFirst() ?: now
                windowNanos - (now - oldest)
            }

            val waitMillis = TimeUnit.NANOSECONDS.toMillis(waitNanos) + 1
            if (waitMillis > remainingMillis) return false

            // Сон происходит БЕЗ УДЕРЖАНИЯ ЛОКА
            try {
                Thread.sleep(minOf(waitMillis, remainingMillis))
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
        }
    }

    private fun tryAcquire(now: Long): Boolean {
        while (dispatches.isNotEmpty() && now - dispatches.peekFirst() >= windowNanos) {
            dispatches.removeFirst()
        }
        if (dispatches.size.toLong() >= rate) return false
        dispatches.addLast(now)
        return true
    }
}