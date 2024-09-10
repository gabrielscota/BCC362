# Usar a imagem oficial do Dart
FROM dart:stable

# Definir o diretório de trabalho dentro do container
WORKDIR /app

# Copiar os arquivos Dart e pubspec.yaml para o container
COPY pubspec.yaml .
COPY bin/sync_coordinator.dart .
COPY service-account.json .

# Instalar as dependências
RUN dart pub get

# Definir o comando para rodar o coordinator
CMD ["dart", "sync_coordinator.dart"]
