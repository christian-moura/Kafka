package br.com.zup.kafka.componentes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    public static final String TYPE_CLASS_CONFIG = "br.com.zup.kafka.componentes.type_config";
    private final Gson gson = new GsonBuilder().create();
    private Class<T> clazz;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
       String clazzName = String.valueOf(configs.get(TYPE_CLASS_CONFIG));
        try {
            this.clazz = (Class<T>) Class.forName(clazzName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Tipo da classe informado para deserialização inexistente.",e);
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return gson.fromJson(new String(bytes), clazz);
    }
}
