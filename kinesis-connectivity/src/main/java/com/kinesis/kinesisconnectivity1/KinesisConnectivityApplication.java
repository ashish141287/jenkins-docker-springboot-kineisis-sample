package com.kinesis.kinesisconnectivity1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class KinesisConnectivityApplication {
	
	public static void main(String[] args) {
		ConfigurableApplicationContext cnf = SpringApplication.run(KinesisConnectivityApplication.class, args);
		KinesisConnection kinesisConnection = cnf.getBean(KinesisConnection.class);
		kinesisConnection.createKinesisConnection();
		//kinesisConnection.pushDataStreams();
		kinesisConnection.getStreamData();
		System.out.println("hello");
	}
}
