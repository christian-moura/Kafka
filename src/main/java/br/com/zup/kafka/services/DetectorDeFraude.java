package br.com.zup.kafka.services;

import br.com.zup.kafka.kafka.KafkaConsumerService;
import br.com.zup.kafka.model.Pedido;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Properties;

public class DetectorDeFraude {
    public static void main(String[] args) {
        var detectorDeFraude = new DetectorDeFraude();
        try(var kafkaService = new KafkaConsumerService(DetectorDeFraude.class.getSimpleName(),
                "LOJA_NOVO_PEDIDO",detectorDeFraude::parse, Pedido.class, Map.of())){
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String,Pedido> record) {
        System.out.println("------------------------------------------------");
        System.out.println("Processando novo pedido, checkando se há fraudes");
        System.out.println("Chave: " + record.key());
        System.out.println("Valor: " + record.value());
        System.out.println("Partição: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Timestamp: " + record.timestamp());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Pedido processado");
        System.out.println("------------------------------------------------");
    }
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DetectorDeFraude.class.getSimpleName());
        return properties;
    }
}
