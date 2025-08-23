# iniciar kafka
docker-compose down -v
docker-compose up -d

# Formatar o codigo
./gradlew ktlintCheck
./gradlew ktlintFormat

