package br.com.palerique.elasticsearch.poc;

import com.github.javafaker.Faker;
import java.util.Locale;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Value;

@Data
@Value
@AllArgsConstructor
@Builder
public class Person {

    Long id;
    String name;
    String house;
    String address;
    String city;
    String dragon;
    String quote;

    public static Person randomPerson() {
        Faker faker = new Faker(new Locale("pt-BR"));

        //        String name = faker.name().fullName();
        String name = faker.gameOfThrones().character();

        return Person.builder()
                .id(faker.number().randomNumber())
                .name(name)
                .house(faker.gameOfThrones().house())
                .address(faker.address().streetAddress())
                .city(faker.gameOfThrones().city())
                .dragon(faker.gameOfThrones().dragon())
                .quote(faker.gameOfThrones().quote())
                .build();
    }
}
