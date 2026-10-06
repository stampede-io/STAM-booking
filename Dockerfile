# Stage 1: build
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY booking/pom.xml .
RUN mvn -q dependency:go-offline
COPY booking/src ./src
RUN mvn -q -DskipTests package

# Stage 2: runtime
FROM eclipse-temurin:21-jre-jammy
# CVE-2026-84782 HIGH in libssl3: the jammy base's apt snapshot lags behind
# Ubuntu's security feed, so pin to the patched build rather than wait for a
# new base image tag.
RUN apt-get update && apt-get install -y --only-upgrade libssl3 openssl \
    && rm -rf /var/lib/apt/lists/*
RUN useradd -r -u 1001 app
USER 1001
WORKDIR /app
COPY --from=build /src/target/*.jar app.jar
ENV JAVA_TOOL_OPTIONS="-Xmx256m -XX:MaxMetaspaceSize=128m"
EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]
