package br.com.zup.kafka.services;

import br.com.zup.kafka.kafka.KafkaConsumerService;
import br.com.zup.kafka.model.Email;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class ServicoDeEmail {
    public static void main(String[] args) {
        var sericoEmail = new ServicoDeEmail();
        try(var kafkaService = new KafkaConsumerService(ServicoDeEmail.class.getSimpleName(),
                    "LOJA_ENVIAR_EMAIL", sericoEmail::parse, Email.class, Map.of())){
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Email> record){
        System.out.println("------------------------------------------------");
        System.out.println("Enviando email");
        System.out.println("Chave: "+record.key());
        System.out.println("Valor: "+record.value());
        System.out.println("Partição: "+record.partition());
        System.out.println("Offset: "+record.offset());
        System.out.println("Timestamp: "+record.timestamp());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email enviado");
        System.out.println("------------------------------------------------");
    }
}
