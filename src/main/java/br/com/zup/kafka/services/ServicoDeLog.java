package br.com.zup.kafka.services;

import br.com.zup.kafka.kafka.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class ServicoDeLog {
    public static void main(String[] args) throws InterruptedException {
        var logService = new ServicoDeLog();
        try(var kafkaService = new KafkaConsumerService(ServicoDeLog.class.getSimpleName(),
                Pattern.compile("LOJA.*"), logService::parse, String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName())
                )){
            kafkaService.run();
        }
    }
    private void parse(ConsumerRecord<String,String> record){

        System.out.println("------------------------------------------------");
        System.out.println("LOG: "+record.topic());
        System.out.print("Chave: "+record.key());
        System.out.print("\tValor: "+record.value());
        System.out.print("\tPartição: "+record.partition());
        System.out.println("\tOffset: "+record.offset());
        System.out.println("\tTimestamp: "+record.timestamp());
        System.out.println("------------------------------------------------");

    }
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ServicoDeLog.class.getSimpleName());
        return properties;
    }
}
