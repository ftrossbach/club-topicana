package com.github.ftrossbach.club_topicana.test;

import com.github.ftrossbach.club_topicana.spring.EnableClubTopicana;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableClubTopicana
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }
}
