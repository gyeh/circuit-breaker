# Circuit-Breaker

Derived from Akka's CircuitBreaker -- minus the Akka dependency with additional enhancements:
* Uses an error percentage threshold over rolling time window to trigger an open state (instead of X number consecutive failures).
* Option to provide a request volume threshold as a prerequisite for open state.

## Sample Usage

```scala
import cb.CircuitBreaker.Configs
import scala.concurrent.Future

val cb = CircuitBreaker("cb", Configs(...))
cb.withCircuitBreaker {
  Future {
    // external call to db, 3rd party service, etc..
  }
}
```
