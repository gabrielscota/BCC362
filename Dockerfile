# Usar a imagem oficial do Dart
FROM dart:stable

# Definir o diretório de trabalho dentro do container
WORKDIR /app

# Copiar os arquivos Dart e pubspec.yaml para o container
COPY pubspec.yaml .
COPY bin/sync_server.dart .
COPY service-account.json .

# Instalar as dependências
RUN dart pub get

# Expor a porta 8080
EXPOSE 8080

# Definir o comando padrão para rodar o servidor sync
CMD ["dart", "sync_server.dart"]
