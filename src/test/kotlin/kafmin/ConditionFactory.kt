package kafmin

import org.awaitility.core.ConditionFactory

fun ConditionFactory.untill(noException: NoException) = this.until {
    var result = true
    try {
        noException.run()
    } catch (e: Throwable) {
        print(e)
        result = false

    }
    result
}

fun interface NoException {
    fun run();
}