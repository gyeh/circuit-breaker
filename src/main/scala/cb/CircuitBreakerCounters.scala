package cb

import java.util.concurrent.atomic.AtomicLong
import cb.CircuitBreakerCounters.HealthCounts

class CircuitBreakerCounters(snapshotInterval: Long, bucketWindowInterval: Int, numberOfBuckets: Int) {

  private val lastHealthCountsSnapshot = new AtomicLong(System.currentTimeMillis())
  @volatile var healthCountsSnapshot = HealthCounts(0, 0, 0)

  private val counter = new RollingNumber(bucketWindowInterval, numberOfBuckets)

  def markSuccess() = counter.increment(RollingNumberEvent.SUCCESS)

  def markFailure() = counter.increment(RollingNumberEvent.FAILURE)

  def getHealthCounts: HealthCounts = {
    val lastTime: Long = lastHealthCountsSnapshot.get
    val currentTime: Long = System.currentTimeMillis
    if (currentTime - lastTime >= snapshotInterval) {
      if (lastHealthCountsSnapshot.compareAndSet(lastTime, currentTime)) {
        val success: Long = counter.getRollingSum(RollingNumberEvent.SUCCESS)
        val failure: Long = counter.getRollingSum(RollingNumberEvent.FAILURE)
        val totalCount: Long = failure + success
        var errorPercentage: Int = 0
        if (totalCount > 0) {
          errorPercentage = (failure.toDouble / totalCount * 100).toInt
        }
        healthCountsSnapshot = HealthCounts(totalCount, failure, errorPercentage)
      }
    }
    healthCountsSnapshot
  }

  def resetCounter() {
    counter.reset()
    lastHealthCountsSnapshot.set(System.currentTimeMillis)
    healthCountsSnapshot = HealthCounts(0, 0, 0)
  }
}

object CircuitBreakerCounters {
  case class HealthCounts(totalCount: Long = 0L, errorCount: Long = 0L, errorPercentage: Int = 0)
}
