package kafmin

data class Reason(val code: String, val message: String, val exception: Throwable?) {
}