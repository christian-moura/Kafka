package br.com.zup.kafka.model;

public class Email {

    private String assunto;
    private String corpo;

    public Email(String assunto, String corpo) {
        this.assunto = assunto;
        this.corpo = corpo;
    }
}
