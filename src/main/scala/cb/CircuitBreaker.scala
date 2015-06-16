/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package cb

import java.util.{TimerTask, Timer}
import java.util.concurrent.atomic.{AtomicReference, AtomicLong, AtomicBoolean}
import cb.CircuitBreaker.Configs
import cb.CircuitBreakerMetrics.MetricSnapshot

import scala.util.control.NoStackTrace
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import scala.concurrent.{ ExecutionContext, Future, Promise, Await }
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.Success

/**
 * Companion object providing factory methods for Circuit Breaker which runs callbacks in caller's thread
 */
object CircuitBreaker {

  /**
   * @param callTimeout Time after which to consider a call a failure
   * @param resetTimeout Time after which to attempt to close the circuit
   * @param errorPercentageThreshold Error threshold to trigger an open circuit-breaker
   * @param requestVolumeThreshold Request volume prequisite to trigger an open circuit-breaker
   * @param snapshotInterval Determines maximum age of a cached snapshot
   * @param bucketWindowInterval Bucket window size in milliseconds.
   * @param numberOfBuckets Number of active buckets within a rolling window.
   */
  case class Configs(callTimeout: FiniteDuration,
                     resetTimeout: FiniteDuration,
                     errorPercentageThreshold: Int = 90,
                     requestVolumeThreshold: Option[Long] = None,
                     snapshotInterval: FiniteDuration,
                     bucketWindowInterval: FiniteDuration,
                     numberOfBuckets: Int = 10)

  private val cache = new ConcurrentHashMap[String, CircuitBreaker]()

  /**
   * Create a new CircuitBreaker.
   *
   * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
   * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
   * executor in the constructor.
   *
   * @param name Unique key of circuit-breaker
   * @param configs Circuit-breaker configs
   */
  def apply(name: String, configs: Configs)(implicit executor: ExecutionContext): CircuitBreaker = {
    val prevBreaker = cache.get(name)
    if (prevBreaker != null) {
      prevBreaker
    } else {
      val prev = cache.putIfAbsent(name,
        new CircuitBreaker(configs))
      if (prev == null) cache.get(name) else prev
    }
  }
}

/**
 * Provides circuit breaker functionality to provide stability when working with "dangerous" operations, e.g. calls to
 * remote systems
 *
 * Transitions through three states:
 * - In *Closed* state, calls pass through until the `maxFailures` count is reached.  This causes the circuit breaker
 * to open.  Both exceptions and calls exceeding `callTimeout` are considered failures.
 * - In *Open* state, calls fail-fast with an exception.  After `resetTimeout`, circuit breaker transitions to
 * half-open state.
 * - In *Half-Open* state, the first call will be allowed through, if it succeeds the circuit breaker will reset to
 * closed state.  If it fails, the circuit breaker will re-open to open state.  All calls beyond the first that
 * execute while the first is running will fail-fast with an exception.
 *
 * @param configs Circuit-breaker configs
 * @param executor [[scala.concurrent.ExecutionContext]] used for execution of state transition listeners
 */
class CircuitBreaker(configs: Configs)(implicit executor: ExecutionContext) {

  private val metrics = new CircuitBreakerMetrics(
    configs.snapshotInterval,
    configs.bucketWindowInterval,
    configs.numberOfBuckets)

  /**
   * Holds reference to current state of CircuitBreaker - *access only via helper methods*
   */
  private[this] val state: AtomicReference[State] = new AtomicReference[State](Closed)

  /**
   * Helper method for access to underlying state
   *
   * @param oldState Previous state on transition
   * @param newState Next state on transition
   * @return Whether the previous state matched correctly
   */
  @inline
  private[this] def swapState(oldState: State, newState: State): Boolean =
    state.compareAndSet(oldState, newState)

  /**
   * Helper method for accessing underlying state
   *
   * @return Reference to current state
   */
  @inline
  private[this] def currentState: State = state.get()

  /**
   * Wraps invocations of asynchronous calls that need to be protected
   *
   * @param body Call needing protected
   * @tparam T return type from call
   * @return [[scala.concurrent.Future]] containing the call result
   */
  def withCircuitBreaker[T](body: ⇒ Future[T]): Future[T] = currentState.invoke(body)

  /**
   * Wraps invocations of synchronous calls that need to be protected
   *
   * Calls are run in caller's thread
   *
   * @param body Call needing protected
   * @tparam T return type from call
   * @return The result of the call
   */
  def withSyncCircuitBreaker[T](body: ⇒ T): T =
    Await.result(
      withCircuitBreaker(try Future.successful(body) catch { case NonFatal(t) ⇒ Future.failed(t) }),
      configs.callTimeout)

  /**
   * Manually force circuit breaker to transition to closed state.
   */
  def forceCloseState(): Unit = state.set(Closed)

  /**
   * Adds a callback to execute when circuit breaker opens
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onOpen(callback: ⇒ Unit): CircuitBreaker = onOpen(new Runnable { def run() = callback })

  /**
   * Java API for onOpen
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onOpen(callback: Runnable): CircuitBreaker = {
    Open addListener callback
    this
  }

  /**
   * Adds a callback to execute when circuit breaker transitions to half-open
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onHalfOpen(callback: ⇒ Unit): CircuitBreaker = onHalfOpen(new Runnable { def run() = callback })

  /**
   * JavaAPI for onHalfOpen
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onHalfOpen(callback: Runnable): CircuitBreaker = {
    HalfOpen addListener callback
    this
  }

  /**
   * Adds a callback to execute when circuit breaker state closes
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onClose(callback: ⇒ Unit): CircuitBreaker = onClose(new Runnable { def run() = callback })

  /**
   * JavaAPI for onClose
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onClose(callback: Runnable): CircuitBreaker = {
    Closed addListener callback
    this
  }

  /**
   * Retrieves current failure count within last window
   *
   * @return count
   */
  def currentFailureCount: Long = Closed.numFailuresInWindow

  /**
   * Implements consistent transition between states
   *
   * @param fromState State being transitioning from
   * @param toState State being transitioning from
   * @throws IllegalStateException if an invalid transition is attempted
   */
  private def transition(fromState: State, toState: State): Unit =
    if (swapState(fromState, toState))
      toState.enter()
    else
      throw new IllegalStateException("Illegal transition attempted from: " + fromState + " to " + toState)

  /**
   * Trips breaker to an open state.  This is valid from Closed or Half-Open states.
   *
   * @param fromState State we're coming from (Closed or Half-Open)
   */
  private def tripBreaker(fromState: State): Unit = transition(fromState, Open)

  /**
   * Resets breaker to a closed state.  This is valid from an Half-Open state only.
   *
   */
  private def resetBreaker(): Unit = transition(HalfOpen, Closed)

  /**
   * Attempts to reset breaker by transitioning to a half-open state.  This is valid from an Open state only.
   *
   */
  private def attemptReset(): Unit = transition(Open, HalfOpen)

  /**
   * Internal state abstraction
   */
  private sealed trait State {
    private val listeners = new CopyOnWriteArrayList[Runnable]

    /**
     * Add a listener function which is invoked on state entry
     *
     * @param listener listener implementation
     */
    def addListener(listener: Runnable): Unit = listeners add listener

    /**
     * Test for whether listeners exist
     *
     * @return whether listeners exist
     */
    private def hasListeners: Boolean = !listeners.isEmpty

    /**
     * Notifies the listeners of the transition event via a Future executed in implicit parameter ExecutionContext
     *
     * @return Promise which executes listener in supplied [[scala.concurrent.ExecutionContext]]
     */
    protected def notifyTransitionListeners() {
      if (hasListeners) {
        val iterator = listeners.iterator
        while (iterator.hasNext) {
          val listener = iterator.next
          executor.execute(listener)
        }
      }
    }

    /**
     * Shared implementation of call across all states.  Thrown exception or execution of the call beyond the allowed
     * call timeout is counted as a failed call, otherwise a successful call
     *
     * @param body Implementation of the call
     * @tparam T Return type of the call's implementation
     * @return Future containing the result of the call
     */
    def callThrough[T](body: ⇒ Future[T]): Future[T] = {
      val deadline = configs.callTimeout.fromNow
      val bodyFuture = try body catch { case NonFatal(t) ⇒ Future.failed(t) }
      bodyFuture.onComplete({
        case s: Success[_] if !deadline.isOverdue() ⇒ callSucceeds()
        case _                                      ⇒ callFails()
      })
      bodyFuture
    }

    /**
     * Abstract entry point for all states
     *
     * @param body Implementation of the call that needs protected
     * @tparam T Return type of protected call
     * @return Future containing result of protected call
     */
    def invoke[T](body: ⇒ Future[T]): Future[T]

    /**
     * Invoked when call succeeds
     *
     */
    def callSucceeds(): Unit

    /**
     * Invoked when call fails
     *
     */
    def callFails(): Unit

    /**
     * Invoked on the transitioned-to state during transition.  Notifies listeners after invoking subclass template
     * method _enter
     *
     */
    final def enter(): Unit = {
      _enter()
      notifyTransitionListeners()
    }

    /**
     * Template method for concrete traits
     *
     */
    def _enter(): Unit
  }

  /**
   * Concrete implementation of Closed state
   */
  private object Closed extends State {
    @volatile var lastSnapshot: Option[MetricSnapshot] = None

    /**
     * Implementation of invoke, which simply attempts the call
     *
     * @param body Implementation of the call that needs protected
     * @tparam T Return type of protected call
     * @return Future containing result of protected call
     */
    override def invoke[T](body: ⇒ Future[T]): Future[T] = callThrough(body)

    /**
     * On successful call, the failure count is reset to 0
     *
     * @return
     */
    override def callSucceeds(): Unit = metrics.markSuccess()

    /**
     * On failed call, the failure count is incremented.  The count is checked against the configured maxFailures, and
     * the breaker is tripped if we have reached maxFailures.
     *
     * @return
     */
    override def callFails(): Unit = {
      metrics.markFailure()

      val snapshot = metrics.getMetricSnapshot
      if ((snapshot.totalCount >= configs.requestVolumeThreshold.getOrElse(-1L))
        && (snapshot.errorPercentage >= configs.errorPercentageThreshold)) {
        tripBreaker(Closed)
      }
      lastSnapshot = Option(snapshot)
    }

    /**
     * On entry of this state, failure count is reset.
     *
     * @return
     */
    override def _enter(): Unit = metrics.reset()

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = {
      s"Closed with failure count in last window = $numFailures"
    }

    def numFailuresInWindow: Long = numFailures

    private def numFailures: Long = {
      lastSnapshot match {
        case Some(s) => s.errorCount
        case None => 0
      }
    }
  }

  /**
   * Concrete implementation of half-open state
   */
  private object HalfOpen extends State {
    private val uninvoked = new AtomicBoolean(true)

    /**
     * Allows a single call through, during which all other callers fail-fast.  If the call fails, the breaker reopens.
     * If the call succeeds the breaker closes.
     *
     * @param body Implementation of the call that needs protected
     * @tparam T Return type of protected call
     * @return Future containing result of protected call
     */
    override def invoke[T](body: ⇒ Future[T]): Future[T] =
      if (uninvoked.compareAndSet(true, false)) callThrough(body) else Promise.failed[T](new CircuitBreakerOpenException(0.seconds)).future

    /**
     * Reset breaker on successful call.
     *
     * @return
     */
    override def callSucceeds(): Unit = resetBreaker()

    /**
     * Reopen breaker on failed call.
     *
     * @return
     */
    override def callFails(): Unit = tripBreaker(HalfOpen)

    /**
     * On entry, guard should be reset for that first call to get in
     *
     * @return
     */
    override def _enter(): Unit = uninvoked.set(true)

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = "Half-Open currently testing call for success = " + uninvoked.get()
  }

  /**
   * Concrete implementation of Open state
   */
  private object Open extends State {
    private val openStartTime = new AtomicLong(0)

    /**
     * Fail-fast on any invocation
     *
     * @param body Implementation of the call that needs protected
     * @tparam T Return type of protected call
     * @return Future containing result of protected call
     */
    override def invoke[T](body: ⇒ Future[T]): Future[T] =
      Promise.failed[T](new CircuitBreakerOpenException(remainingDuration())).future

    /**
     * Calculate remaining duration until reset to inform the caller in case a backoff algorithm is useful
     *
     * @return duration to when the breaker will attempt a reset by transitioning to half-open
     */
    private def remainingDuration(): FiniteDuration = {
      val diff = System.nanoTime() - openStartTime.get
      if (diff <= 0L) Duration.Zero
      else diff.nanos
    }

    /**
     * No-op for open, calls are never executed so cannot succeed or fail
     *
     * @return
     */
    override def callSucceeds(): Unit = ()

    /**
     * No-op for open, calls are never executed so cannot succeed or fail
     *
     * @return
     */
    override def callFails(): Unit = ()

    /**
     * On entering this state, schedule an attempted reset and store the entry time to
     * calculate remaining time before attempted reset.
     *
     * @return
     */
    override def _enter(): Unit = {
      openStartTime.set(System.nanoTime())
      new Scheduler(configs.resetTimeout).start()
    }

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = "Open"
  }

  private class Scheduler(duration: Duration) {
    val timer = new Timer()

    def start(): Unit = {
      timer.schedule(new ResetTask(), duration.toMillis)
    }

    private class ResetTask extends TimerTask {
      def run() {
        attemptReset()
        timer.cancel()
      }
    }
  }

}

/**
 * Exception thrown when Circuit Breaker is open.
 *
 * @param remainingDuration Stores remaining time before attempting a reset.  Zero duration means the breaker is
 *                          currently in half-open state.
 * @param message Defaults to "Circuit Breaker is open; calls are failing fast"
 */
class CircuitBreakerOpenException(val remainingDuration: FiniteDuration,
                                  message: String = "Circuit Breaker is open; calls are failing fast")
  extends RuntimeException(message) with NoStackTrace
