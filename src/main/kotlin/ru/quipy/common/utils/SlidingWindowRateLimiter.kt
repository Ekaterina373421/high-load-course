package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
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
        if (!lock.tryLock(0, TimeUnit.NANOSECONDS)) return false
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
        val remainingMillis = deadlineAt - System.currentTimeMillis()
        if (remainingMillis <= 0 || !lock.tryLock(remainingMillis, TimeUnit.MILLISECONDS)) return false
        try {
            while (true) {
                val remaining = deadlineAt - System.currentTimeMillis()
                if (remaining <= 0) return false
                val now = System.nanoTime()
                if (tryAcquire(now)) return true
                val waitNanos = windowNanos - (now - dispatches.peekFirst())
                TimeUnit.NANOSECONDS.sleep(
                    minOf(waitNanos, TimeUnit.MILLISECONDS.toNanos(remaining))
                )
            }
        } finally {
            lock.unlock()
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