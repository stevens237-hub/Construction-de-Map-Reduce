FROM eclipse-temurin:17-jdk

WORKDIR /app

# Copier toutes les sources Java (common + coordinator + les autres)
COPY src/ ./src/

# Compiler tout le projet en une seule passe
RUN mkdir -p classes && find src -name "*.java" | xargs javac -d classes

# Répertoire pour les fichiers texte (monté via volume)
RUN mkdir -p texts

CMD ["java", "-cp", "classes", "coordinator.Coordinator"]