package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.apigateway.APIController
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentFlowMetrics: PaymentFlowMetrics

    @Autowired
    private lateinit var orderRepository: OrderRepository

    private val paymentExecutor = ThreadPoolExecutor(
        200,
        200,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    /**
     * Резервируем место во входном оконном meter до любых DB-операций.
     * Всплеск допускается целиком, если его можно отправить до дедлайна.
     */
    fun tryAdmit(deadline: Long): PaymentAdmission = paymentService.tryAdmit(deadline)

    fun processPayment(orderId: UUID, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        paymentFlowMetrics.received()
        paymentExecutor.submit {
            val order = orderRepository.findById(orderId)
                ?: throw IllegalArgumentException("No such order $orderId")
            orderRepository.save(order.copy(status = APIController.OrderStatus.PAYMENT_IN_PROGRESS))

            val createdEvent = paymentESService.create {
                it.create(paymentId, orderId, order.price)
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
            paymentService.submitPaymentRequest(paymentId, order.price, createdAt, deadline)
        }
        return createdAt
    }

    fun canAcceptPayment(): Boolean = paymentService.canAcceptPayment()
}