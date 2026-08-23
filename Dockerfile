FROM maven:3.9-eclipse-temurin-26 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn package -DskipTests

FROM eclipse-temurin:26-jre-alpine
WORKDIR /app
COPY --from=build /app/target/LaraMQ.jar app.jar
EXPOSE 3000
ENTRYPOINT ["java", "-jar", "app.jar", "--host=0.0.0.0", "--port=3000"]
