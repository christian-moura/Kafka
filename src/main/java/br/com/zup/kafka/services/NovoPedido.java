package br.com.zup.kafka.services;

import br.com.zup.kafka.kafka.KafkaProducerService;
import br.com.zup.kafka.model.Email;
import br.com.zup.kafka.model.Pedido;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NovoPedido {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var pedidoProducer = new KafkaProducerService<Pedido>()) {
            try (var emailProducer = new KafkaProducerService<Email>()) {
                var oderId = UUID.randomUUID().toString();
                var userID = UUID.randomUUID().toString();
                var value = Math.random() * 5000 + 1;
                Pedido pedido = new Pedido(userID, oderId, new BigDecimal(value));
                pedidoProducer.send("LOJA_NOVO_PEDIDO", userID, pedido);
                Email email = new Email("Novo Pedido","Obrigado! Estamos processando seu pedido!");
                emailProducer.send("LOJA_ENVIAR_EMAIL", userID, email);
            }
        }
    }
}
