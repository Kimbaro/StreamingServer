FROM openjdk:11
ARG JAR_FILE=/HubConnector-0.0.1-SNAPSHOT.jar
COPY ${JAR_FILE} HubConnector-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","/HubConnector-0.0.1-SNAPSHOT.jar"]
EXPOSE 8080