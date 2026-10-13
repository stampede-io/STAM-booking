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
# STAM-68 AC1: the OTel Java agent, not an in-process SDK -- no-code
# instrumentation, auto-attached via -javaagent below. --chmod since ADD
# from a URL defaults to 0600, unreadable by the non-root user switched to
# two lines down.
ADD --chmod=644 https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.26.1/opentelemetry-javaagent.jar /otel-javaagent.jar
USER 1001
WORKDIR /app
COPY --from=build /src/target/*.jar app.jar
ENV JAVA_TOOL_OPTIONS="-Xmx256m -XX:MaxMetaspaceSize=224m -javaagent:/otel-javaagent.jar"
EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]
