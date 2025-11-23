FROM gcr.io/distroless/java17-debian12
ARG FINAL_NAME
COPY target/${FINAL_NAME}.jar /app/SpringBoot.jar
ENTRYPOINT ["java", "-jar", "/app/SpringBoot.jar"]
