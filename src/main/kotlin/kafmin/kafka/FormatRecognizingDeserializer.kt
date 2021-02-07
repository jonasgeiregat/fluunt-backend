package kafmin.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.serialization.Deserializer
import java.nio.ByteBuffer

class FormatRecognizingDeserializer : Deserializer<String> {
    private val AVRO_MAGIC_BYTE: Byte = 0x0

    override fun deserialize(topic: String, data: ByteArray): String {
        val bytes = ByteBuffer.wrap(data)
        if(bytes.get() == AVRO_MAGIC_BYTE) {
            val deserializer = KafkaAvroDeserializer(CachedSchemaRegistryClient("http://localhost:8081", 1000))
            val value = deserializer.deserialize(topic, data)
            return value.toString()
        }
        return String(data)
    }

}
