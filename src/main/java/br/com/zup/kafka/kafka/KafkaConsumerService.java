package br.com.zup.kafka.kafka;

import br.com.zup.kafka.componentes.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaConsumerService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    private KafkaConsumerService(String groupId, ConsumerFunction parse, Class<T> typeClass, Map<String,String> extraProperties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(typeClass, groupId, extraProperties));
    }

    public KafkaConsumerService(String groupId , String topico, ConsumerFunction parse,  Class<T> typeClass, Map<String,String> extraProperties) {
        this(groupId,parse, typeClass,extraProperties);
        consumer.subscribe(Collections.singletonList(topico));
    }

    public KafkaConsumerService(String groupId, Pattern topico, ConsumerFunction parse, Class<T> typeClass, Map<String,String> extraProperties) {
        this(groupId,parse, typeClass, extraProperties);
        consumer.subscribe(topico);
    }

    public void run(){
        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println("Foram encontrados "+records.count()+ " registros");
                for(var record : records){
                    parse.consume(record);
                }
            }
        }
    }

    private Properties properties(Class<T> typeClass, String groupId, Map<String, String> extraProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CLASS_CONFIG, typeClass.getName());
        properties.putAll(extraProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
