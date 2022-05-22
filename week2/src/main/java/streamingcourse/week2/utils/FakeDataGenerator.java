package streamingcourse.week2.utils;

import com.github.javafaker.Faker;

public class FakeDataGenerator {
    public static void main(String[] args) {
        Faker faker = new Faker();
        for (int i = 0; i < 100; i++) {
            System.out.printf("%d: %s\n", i, faker.backToTheFuture().quote());
            System.out.printf("%d: %s\n", i, faker.buffy().quotes());
            System.out.println();
        }
    }
}
