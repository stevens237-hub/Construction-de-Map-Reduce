FROM eclipse-temurin:17-jdk

WORKDIR /app

COPY src/ ./src/

RUN mkdir -p classes && find src -name "*.java" | xargs javac -d classes

CMD ["java", "-cp", "classes", "reduceworker.ReduceWorker"]