package cb

import java.util.concurrent.atomic.AtomicLong

import cb.CircuitBreakerMetrics.MetricSnapshot

import scala.concurrent.duration.FiniteDuration

/**
 * Encapsulates circuit breaker-specific metrics with a rolling window counter.
 * Measures the number of successes and failures over a configurable time period.
 *
 * @param snapshotInterval Determines maximum age of a cached snapshot
 * @param bucketWindowInterval Bucket window size
 * @param numberOfBuckets Number of active buckets within a rolling window.
 */
class CircuitBreakerMetrics(snapshotInterval: FiniteDuration,
                            bucketWindowInterval: FiniteDuration,
                            numberOfBuckets: Int) {

  private val lastSnapshotTimestamp = new AtomicLong(System.currentTimeMillis())
  @volatile private var lastSnapshot = MetricSnapshot(0, 0, 0)

  private val counter = new RollingNumber(bucketWindowInterval.toMillis.toInt, numberOfBuckets)

  /**
   * Records a single success event.
   */
  def markSuccess(): Unit = counter.increment(RollingNumberEvent.SUCCESS)

  /**
   * Records a single failure event.
   */
  def markFailure(): Unit = counter.increment(RollingNumberEvent.FAILURE)

  /**
   * Returns a snapshot of the rolling window and cached for subsequent queries.
   */
  def getMetricSnapshot: MetricSnapshot = {
    val lastTime: Long = lastSnapshotTimestamp.get
    val currentTime: Long = System.currentTimeMillis
    if (currentTime - lastTime >= snapshotInterval.toMillis) {
      if (lastSnapshotTimestamp.compareAndSet(lastTime, currentTime)) {
        val success: Long = counter.getRollingSum(RollingNumberEvent.SUCCESS)
        val failure: Long = counter.getRollingSum(RollingNumberEvent.FAILURE)
        val totalCount: Long = failure + success
        var errorPercentage: Int = 0
        if (totalCount > 0) {
          errorPercentage = (failure.toDouble / totalCount * 100).toInt
        }
        lastSnapshot = MetricSnapshot(totalCount, failure, errorPercentage)
      }
    }
    lastSnapshot
  }

  /**
   * Resets the rolling window.
   */
  def reset(): Unit = {
    counter.reset()
    lastSnapshotTimestamp.set(System.currentTimeMillis)
    lastSnapshot = MetricSnapshot(0, 0, 0)
  }
}

object CircuitBreakerMetrics {
  case class MetricSnapshot(totalCount: Long = 0L, errorCount: Long = 0L, errorPercentage: Int = 0)
}
