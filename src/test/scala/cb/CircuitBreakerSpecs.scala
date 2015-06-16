package cb

import java.util.concurrent.CountDownLatch

import cb.CircuitBreaker.Configs
import org.specs2.SpecificationLike
import org.specs2.execute.Success

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


class CircuitBreakerSpecs
  extends SpecificationLike { override def is = s2"""

  Specs for modified CircuitBreaker

  A synchronous circuit breaker that is open should:
    throw exceptions when called before reset timeout                         ${SyncOpen().throwBeforeReset}
    be in half-open state after reset timeout                                 ${SyncOpen().resetTimeout}

  A synchronous circuit breaker that is half-open should:
    transition to close state after success                                   ${SyncHalfOpen().closeOnSuccess}
    transition to open state after failure                                    ${SyncHalfOpen().openOnFail}

  A synchronous circuit breaker that is closed should:
    allow non-error requests                                                  ${SyncClose().allowRequest}
    increment failure count when exception is thrown                          ${SyncClose().incrementFailureCount}
    increment failure count when timeout occurs                               ${SyncClose().incrementFailureCountDueToTimeout}
    transition to open if error % is exceeded                                 ${SyncClose().errorPercentageExceeded}
    remain in close state if error % is not exceeded                          ${SyncClose().errorPercentageNotExceeded}
    not transition to open if error % is exceeded but request volume req is not met ${SyncClose().errorPercentageExceededButUnderRequestVolume}

  An asynchronous circuit breaker that is open should:
    throw exceptions when called before reset timeout                         ${AsyncOpen().throwBeforeReset}
    be in half-open state after reset timeout                                 ${AsyncOpen().resetTimeout}

  An asynchronous circuit breaker that is half-open should:
    transition to close state after success                                   ${AsyncHalfOpen().closeOnSuccess}
    transition to open state after failure                                    ${AsyncHalfOpen().openOnFail}

  An asynchronous circuit breaker that is closed should:
    allow non-error requests                                                  ${AsyncClose().allowRequest}
    increment failure count when exception is thrown                          ${AsyncClose().incrementFailureCount}
    increment failure count when timeout occurs                               ${AsyncClose().incrementFailureCountDueToTimeout}
    transition to open if error % is exceeded                                 ${AsyncClose().errorPercentageExceeded}
    remain in close state if error % is not exceeded                          ${AsyncClose().errorPercentageNotExceeded}
    not transition to open if error % is exceeded but request volume req is not met ${AsyncClose().errorPercentageExceededButUnderRequestVolume}

  """

  sealed trait Context {
    class TestException extends RuntimeException

    class Breaker(val instance: CircuitBreaker) {
      val halfOpenLatch = new CountDownLatch(1)
      val openLatch = new CountDownLatch(1)
      val closedLatch = new CountDownLatch(1)
      instance.onClose(closedLatch.countDown()).onHalfOpen(halfOpenLatch.countDown()).onOpen(openLatch.countDown())
    }

    val awaitTimeout = 2.seconds

    def checkLatch(latch: CountDownLatch) = {
      latch.await(2, SECONDS)
      success
    }

    def throwException = throw new TestException

    def createBreaker(configs: Configs): Breaker = {
      new Breaker(new CircuitBreaker(configs))
    }

    /**
     * Create a breaker in closed state
     */
    def createCloseBreaker(callTimeout: FiniteDuration,
                           resetTimeout: FiniteDuration,
                           errorPercentageThreshold: Int = 50,
                           requestVolumeThreshold: Option[Long] = None): Breaker = {
      val breaker = createBreaker(Configs(
        callTimeout,
        resetTimeout,
        errorPercentageThreshold = errorPercentageThreshold,
        requestVolumeThreshold = requestVolumeThreshold,
        snapshotInterval = FiniteDuration(0, MILLISECONDS),
        bucketWindowInterval = FiniteDuration(10000, MILLISECONDS),
        numberOfBuckets = 10
      ))

      breaker
    }

    /**
     * Create a breaker open state
     */
    def createOpenBreaker(callTimeout: FiniteDuration,
                          resetTimeout: FiniteDuration): Breaker = {
      val breaker = createCloseBreaker(callTimeout, resetTimeout)

      // get into open state
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      checkLatch(breaker.openLatch)

      breaker
    }
  }

  case class SyncOpen() extends Context {

    def throwBeforeReset = {
      val breaker = createOpenBreaker(100.millis, 5.seconds)
      Try(breaker.instance.withSyncCircuitBreaker("sayHi")) must beAFailedTry[String].withThrowable[CircuitBreakerOpenException]
    }

    def resetTimeout = {
      val breaker = createOpenBreaker(1000.millis, 50.millis)
      checkLatch(breaker.halfOpenLatch)
    }
  }

  case class SyncHalfOpen() extends Context {
    def closeOnSuccess = {
      val breaker = createOpenBreaker(1000.millis, 50.millis)
      checkLatch(breaker.halfOpenLatch)
      breaker.instance.withSyncCircuitBreaker("sayHi")
      checkLatch(breaker.closedLatch)
    }

    def openOnFail = {
      val breaker = createOpenBreaker(1000.millis, 50.millis)
      checkLatch(breaker.halfOpenLatch)
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      checkLatch(breaker.openLatch)
    }
  }

  case class SyncClose() extends Context {
    def allowRequest = {
      val breaker = createCloseBreaker(100.millis, 5.seconds)
      breaker.instance.withSyncCircuitBreaker("sayHi") mustEqual "sayHi"
    }

    def incrementFailureCount = {
      val breaker = createCloseBreaker(100.millis, 5.seconds)
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      breaker.instance.currentFailureCount mustEqual 1
    }

    def incrementFailureCountDueToTimeout = {
      val breaker = createCloseBreaker(50.millis, 500.millis)
      Try(breaker.instance.withSyncCircuitBreaker(Thread.sleep(200.millis.toMillis)))
      breaker.instance.currentFailureCount mustEqual 1
    }

    def errorPercentageExceeded = {
      val breaker = createCloseBreaker(50.millis, 500.millis, errorPercentageThreshold = 65, requestVolumeThreshold = Some(3))
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      breaker.instance.withSyncCircuitBreaker("sayHi")
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      checkLatch(breaker.openLatch)
    }

    def errorPercentageNotExceeded = {
      val breaker = createCloseBreaker(50.millis, 500.millis, errorPercentageThreshold = 65, requestVolumeThreshold = Some(5))
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      breaker.instance.withSyncCircuitBreaker("sayHi")
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      breaker.instance.withSyncCircuitBreaker("sayHi")
      Try(breaker.instance.withSyncCircuitBreaker(throwException))

      Try(checkLatch(breaker.openLatch)) must beAFailedTry[Success].withThrowable[java.util.concurrent.TimeoutException]
    }

    def errorPercentageExceededButUnderRequestVolume = {
      val breaker = createCloseBreaker(50.millis, 500.millis, errorPercentageThreshold = 65, requestVolumeThreshold = Some(2))
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      breaker.instance.withSyncCircuitBreaker("sayHi")
      Try(breaker.instance.withSyncCircuitBreaker(throwException))
      checkLatch(breaker.openLatch)
    }
  }

  case class AsyncOpen() extends Context {

    def throwBeforeReset = {
      val breaker = createOpenBreaker(100.millis, 5.seconds)
      Try(Await.result(breaker.instance.withCircuitBreaker(Future("sayHi")), awaitTimeout)) must beAFailedTry[String].withThrowable[CircuitBreakerOpenException]
    }

    def resetTimeout = {
      val breaker = createOpenBreaker(1000.millis, 50.millis)
      checkLatch(breaker.halfOpenLatch)
    }
  }

  case class AsyncHalfOpen() extends Context {
    def closeOnSuccess = {
      val breaker = createOpenBreaker(1000.millis, 50.millis)
      checkLatch(breaker.halfOpenLatch)
      breaker.instance.withCircuitBreaker(Future("sayHi"))
      checkLatch(breaker.closedLatch)
    }

    def openOnFail = {
      val breaker = createOpenBreaker(1000.millis, 50.millis)
      checkLatch(breaker.halfOpenLatch)
      Try(breaker.instance.withCircuitBreaker(Future(throwException)))
      checkLatch(breaker.openLatch)
    }
  }

  case class AsyncClose() extends Context {
    def allowRequest = {
      val breaker = createCloseBreaker(100.millis, 5.seconds)
      Await.result(breaker.instance.withCircuitBreaker(Future("sayHi")), awaitTimeout) mustEqual "sayHi"
    }

    def incrementFailureCount = {
      val breaker = createCloseBreaker(100.millis, 5.seconds)
      Try(breaker.instance.withCircuitBreaker(throwException))
      checkLatch(breaker.openLatch)
      breaker.instance.currentFailureCount mustEqual 1
    }

    def incrementFailureCountDueToTimeout = {
      val breaker = createCloseBreaker(50.millis, 500.millis)
      Try(breaker.instance.withCircuitBreaker(Future(Thread.sleep(200.millis.toMillis))))
      checkLatch(breaker.openLatch)
      breaker.instance.currentFailureCount mustEqual 1
    }

    def errorPercentageExceeded = {
      val breaker = createCloseBreaker(50.millis, 500.millis, errorPercentageThreshold = 65, requestVolumeThreshold = Some(3))
      Try(breaker.instance.withCircuitBreaker(Future(throwException)))
      breaker.instance.withCircuitBreaker(Future("sayHi"))
      Try(breaker.instance.withCircuitBreaker(Future(throwException)))
      checkLatch(breaker.openLatch)
    }

    def errorPercentageNotExceeded = {
      val breaker = createCloseBreaker(50.millis, 500.millis, errorPercentageThreshold = 65, requestVolumeThreshold = Some(5))
      Try(breaker.instance.withCircuitBreaker(Future(throwException)))
      breaker.instance.withCircuitBreaker(Future("sayHi"))
      Try(breaker.instance.withCircuitBreaker(Future(throwException)))
      breaker.instance.withCircuitBreaker(Future("sayHi"))
      Try(breaker.instance.withCircuitBreaker(Future(throwException)))

      Try(checkLatch(breaker.openLatch)) must beAFailedTry[Success].withThrowable[java.util.concurrent.TimeoutException]
    }

    def errorPercentageExceededButUnderRequestVolume = {
      val breaker = createCloseBreaker(50.millis, 500.millis, errorPercentageThreshold = 65, requestVolumeThreshold = Some(2))
      Try(breaker.instance.withCircuitBreaker(Future(throwException)))
      breaker.instance.withCircuitBreaker(Future("sayHi"))
      Try(breaker.instance.withCircuitBreaker(Future(throwException)))
      checkLatch(breaker.openLatch)
    }
  }
}

