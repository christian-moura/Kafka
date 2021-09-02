package br.com.zup.kafka.model;

import java.math.BigDecimal;

public class Pedido {

    private String userID;
    private String orderID;
    private BigDecimal valor;

    public Pedido(String userID, String orderID, BigDecimal valor) {
        this.userID = userID;
        this.orderID = orderID;
        this.valor = valor;
    }
}
