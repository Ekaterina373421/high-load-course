package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.floor


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val paymentFlowMetrics: PaymentFlowMetrics,
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    // admission рассчитывается по самому медленному из аккаунтов, а не по сумме их мощностей.
    private val enabledAccounts = paymentAccounts.filter { it.isEnabled() }
    private val processingReserveMillis = enabledAccounts.maxOfOrNull {
        it.processingReserveMillis()
    } ?: 0L
    private val throughput = enabledAccounts.minOfOrNull {
        val p = it.accountProperties()
        minOf(
            p.rateLimitPerSec.toDouble(),
            p.parallelRequests * 1000.0 / p.averageProcessingTime.toMillis().coerceAtLeast(1)
        )
    } ?: 0.0
    private val admissionIntervalNanos =
        if (throughput > 0) SlidingWindowRateLimiter.intervalNanos(throughput) else 0L

    private val preparationReserveMillis = TimeUnit.NANOSECONDS.toMillis(admissionIntervalNanos) +
        if (admissionIntervalNanos % 1_000_000L == 0L) 0L else 1L
    private val admissionWindowNanos =
        TimeUnit.MILLISECONDS.toNanos(SlidingWindowRateLimiter.WINDOW_MILLIS.toLong())

    private val burstCapacity = minOf(
        floor(throughput).toInt(),
        enabledAccounts.minOfOrNull { it.accountProperties().parallelRequests } ?: 0,
    ).coerceAtLeast(0)

    private val admissions = ArrayDeque<Long>()

    /**
     * виртуальный оконный meter: первая пачка доступна сразу, следующие - по окнам.
     * дедлайн ограничивает накопление будущих пачек
     */
    @Synchronized
    override fun tryAdmit(deadlineAt: Long): PaymentAdmission {
        val nowMillis = System.currentTimeMillis()
        if (burstCapacity <= 0) {
            logger.warn("Admission rejected: no capacity, deadlineAt={}, remainingMillis={}", deadlineAt, deadlineAt - nowMillis)
            return rejectAdmission(nowMillis, TimeUnit.SECONDS.toNanos(1))
        }
        val remainingMillis = deadlineAt - nowMillis -
            processingReserveMillis - preparationReserveMillis
        if (remainingMillis <= 0) {
            logger.info(
                "Admission rejected: insufficient budget, deadlineAt={}, remainingMillis={}, allowedWaitMillis={}",
                deadlineAt, deadlineAt - nowMillis, remainingMillis,
            )
            return rejectAdmission(nowMillis, admissionIntervalNanos)
        }
        val current = System.nanoTime()
        val scheduledAt = if (admissions.size < burstCapacity) current else
            maxOf(current, admissions.first() + admissionWindowNanos)
        val allowedWaitNanos = TimeUnit.MILLISECONDS.toNanos(remainingMillis)
        val backlogNanos = scheduledAt - current
        if (backlogNanos > allowedWaitNanos) {
            logger.info(
                "Admission rejected: window full, deadlineAt={}, remainingMillis={}, virtualWaitMillis={}, allowedWaitMillis={}, reservedSlots={}",
                deadlineAt, deadlineAt - nowMillis, TimeUnit.NANOSECONDS.toMillis(backlogNanos),
                remainingMillis, admissions.size,
            )
            // ждем освобождения бакета, но не менее одного интервала утечки
            return rejectAdmission(nowMillis, maxOf(admissionIntervalNanos, backlogNanos - allowedWaitNanos))
        }
        if (admissions.size == burstCapacity) admissions.removeFirst()
        admissions.addLast(scheduledAt)
        return PaymentAdmission.Accepted
    }

    private fun rejectAdmission(nowMillis: Long, waitNanos: Long): PaymentAdmission.Rejected {
        paymentFlowMetrics.rejectedAdmission()
        val waitMillis = TimeUnit.NANOSECONDS.toMillis(waitNanos) +
            if (waitNanos % 1_000_000L == 0L) 0L else 1L
        val retryAtMillis = nowMillis + waitMillis.coerceAtLeast(1)
        logger.info("Admission feedback: retryAtMillis={}, waitMillis={}", retryAtMillis, waitMillis)
        return PaymentAdmission.Rejected(retryAtMillis)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    override fun canAcceptPayment(): Boolean {
        return paymentAccounts.any { it.canAcceptPayment() }
    }
}