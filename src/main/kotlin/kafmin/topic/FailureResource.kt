package kafmin.topic

import io.micronaut.core.annotation.Introspected
import kafmin.Reason

@Introspected
class FailureResource(reason: Reason) {
    val code = reason.code
    val message = reason.message
}
