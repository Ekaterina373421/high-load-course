package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class OngoingWindow(
    maxWinSize: Int
) {
    private val window = Semaphore(maxWinSize)

    fun acquire() {
        window.acquire()
    }

    // кейс 3: захватываем слот с таймаутом
    fun acquire(timeout: Duration): Boolean {
        if (timeout.isNegative || timeout.isZero) return false
        return window.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)
    }

    fun release() = window.release()

    fun awaitingQueueSize() = window.queueLength
}

class NonBlockingOngoingWindow(
    private val maxWinSize: Int
) {
    private val winSize = AtomicInteger()

    fun putIntoWindow(): WindowResponse {
        while (true) {
            val currentWinSize = winSize.get()
            if (currentWinSize >= maxWinSize) {
                return WindowResponse.Fail(currentWinSize)
            }

            if (winSize.compareAndSet(currentWinSize, currentWinSize + 1)) {
                break
            }
        }
        return WindowResponse.Success(winSize.get())
    }

    fun releaseWindow() = winSize.decrementAndGet()


    sealed class WindowResponse(val currentWinSize: Int) {
        public class Success(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)

        public class Fail(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)
    }
}