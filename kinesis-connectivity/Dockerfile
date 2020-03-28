FROM amazoncorretto
VOLUME /tmp
ADD target/kinesis-connectivity-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"] 