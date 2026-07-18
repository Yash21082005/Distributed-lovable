package com.codingshuttle.distributed_lovable.account_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

//@SpringBootApplication

@SpringBootApplication(
        scanBasePackages = {
                "com.codingshuttle.distributed_lovable.account_service",
                "com.codingshuttle.distributed_lovable.common_lib"
        }
)
public class AccountServiceApplication {

	public static void main(String[] args) {

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));

        SpringApplication.run(AccountServiceApplication.class, args);

	}

}
