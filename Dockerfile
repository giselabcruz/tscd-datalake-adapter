FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

COPY ingestion-app/pom.xml .
RUN mvn -q -e -DskipTests dependency:go-offline

COPY ingestion-app/src ./src
RUN mvn -q -e -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN useradd -r -u 10001 appuser
USER appuser
COPY --from=build /build/target/*.jar /app/app.jar
EXPOSE 7070
ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]