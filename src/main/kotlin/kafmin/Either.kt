package kafmin


sealed class Result<FAILURE, SUCCESS> {
}

class Failed<FAILURE, SUCCESS>(val faiure: FAILURE) : Result<FAILURE, SUCCESS>()
class Succeeded<FAILURE, SUCCESS>(val success: SUCCESS) : Result<FAILURE, SUCCESS>()

