import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:googleapis/pubsub/v1.dart' as pubsub;
import 'package:googleapis/pubsub/v1.dart';
import 'package:googleapis_auth/auth_io.dart';
import 'package:logging/logging.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as io;

// Fila de mensagens para garantir FIFO
final Queue<Map<String, dynamic>> messageQueue = Queue();

void setupLogging() {
  // Configura o formato dos logs
  Logger.root.level = Level.ALL;
  Logger.root.onRecord.listen((record) {
    print('[${record.loggerName}] [${record.level.name}]: ${record.message}');
  });
}

Future<pubsub.PubsubApi> createPubSubClient() async {
  var accountCredentials = ServiceAccountCredentials.fromJson(File('service-account.json').readAsStringSync());

  var scopes = [pubsub.PubsubApi.cloudPlatformScope];

  var authClient = await clientViaServiceAccount(accountCredentials, scopes);
  return pubsub.PubsubApi(authClient);
}

class SyncServer {
  final String serverId;
  final String topic;
  final String subscription;
  final pubsub.PubsubApi pubSubClient;
  final Logger logger;

  SyncServer({
    required this.serverId,
    required this.topic,
    required this.subscription,
    required this.pubSubClient,
    required this.logger,
  });

  // Envia uma mensagem de ACQUIRE ao Pub/Sub com timestamp
  Future<void> publishAcquireMessage(int acquireTimestamp) async {
    final message = json.encode({
      'serverId': serverId,
      'action': 'ACQUIRE',
      'timestamp': acquireTimestamp, // Mantém o timestamp de ACQUIRE
    });
    await pubSubClient.projects.topics.publish(
      PublishRequest(
        messages: [PubsubMessage(data: base64Encode(utf8.encode(message)))],
      ),
      topic,
    );
    logger.info('Published ACQUIRE message: $message');
  }

  // Envia uma mensagem de RELEASE ao Pub/Sub, incluindo o timestamp original de ACQUIRE
  Future<void> publishReleaseMessage(int acquireTimestamp) async {
    var message = json.encode({
      'serverId': serverId,
      'action': 'RELEASE',
      'timestamp': acquireTimestamp, // Usa o timestamp de ACQUIRE
    });
    await pubSubClient.projects.topics.publish(
      PublishRequest(
        messages: [PubsubMessage(data: base64Encode(utf8.encode(message)))],
      ),
      topic,
    );
    logger.info('Published RELEASE message: $message');
  }

  // Escuta mensagens do Pub/Sub e processa as de ACQUIRE e RELEASE
  Future<bool> listenForMessages(int acquireTimestamp) async {
    while (true) {
      var response = await pubSubClient.projects.subscriptions.pull(
        PullRequest(returnImmediately: false, maxMessages: 10),
        subscription,
      );
      for (var receivedMessage in response.receivedMessages!) {
        var message = utf8.decode(base64Decode(receivedMessage.message!.data!));

        try {
          var decodedMessage = json.decode(message);

          if (decodedMessage['action'] == 'ACQUIRE') {
            // Verifica se o servidor pode acessar a seção crítica
            if (_canAccessCriticalSection(decodedMessage)) {
              // Acessa o recurso
              await accessResource();

              // Após acessar o recurso, envia uma mensagem de RELEASE
              await publishReleaseMessage(acquireTimestamp);

              // Espera até que o RELEASE seja recebido
              await waitForRelease(acquireTimestamp);

              return true; // Retorna sucesso após o RELEASE
            }
          } else if (decodedMessage['action'] == 'RELEASE') {}
        } catch (e) {
          logger.severe('Error decoding message: $e');
        }

        // Confirma que a mensagem foi processada
        await pubSubClient.projects.subscriptions.acknowledge(
          AcknowledgeRequest(ackIds: [receivedMessage.ackId!]),
          subscription,
        );
      }
    }
  }

  // Verifica se o servidor pode acessar a seção crítica com base nas mensagens do Pub/Sub
  bool _canAccessCriticalSection(Map<String, dynamic> decodedMessage) {
    var serverId = decodedMessage['serverId'];
    var action = decodedMessage['action'];
    var messageTimestamp = decodedMessage['timestamp'];

    if (action == 'ACQUIRE' && serverId == this.serverId) {
      logger.info('Received ACQUIRE message from server $serverId with timestamp $messageTimestamp');

      // Adiciona a mensagem à fila de mensagens ACQUIRE
      messageQueue.add(decodedMessage);
      logger.info(
          'Added message ($messageTimestamp) from server $serverId to the queue. Queue size: ${messageQueue.length}');

      // Verifica se o primeiro item na fila é o servidor atual
      if (messageQueue.isNotEmpty && messageQueue.first['timestamp'] == messageTimestamp) {
        logger.info('Server $serverId has access to the critical section');
        return true; // O servidor pode acessar a seção crítica
      }
    } else if (action == 'RELEASE' && serverId == this.serverId) {
      logger.info('Received RELEASE message ($messageTimestamp) from server $serverId');

      // Remove a mensagem correspondente da fila comparando id e timestamp de ACQUIRE
      messageQueue.removeWhere((msg) =>
          msg['serverId'] == serverId &&
          msg['timestamp'] == messageTimestamp); // Comparar timestamp da mensagem de ACQUIRE

      logger.info(
          'Removed message from server $serverId with timestamp $messageTimestamp from the queue. Queue size: ${messageQueue.length}');
      return true;
    }

    return false;
  }

  // Simula o acesso ao recurso
  Future<void> accessResource() async {
    logger.info('Server $serverId accessing the critical section...');
    int criticalRegionTime = 200 + Random().nextInt(801); // Tempo de acesso aleatório entre 200 e 1000ms
    await Future.delayed(Duration(milliseconds: criticalRegionTime)); // Reduzi o tempo para milissegundos
    logger.info('Server $serverId left the critical section.');
  }

  // Escuta mensagens do Pub/Sub e espera até a mensagem de RELEASE
  Future<void> waitForRelease(int acquireTimestamp) async {
    while (true) {
      var response = await pubSubClient.projects.subscriptions.pull(
        PullRequest(returnImmediately: false, maxMessages: 10),
        subscription,
      );
      for (var receivedMessage in response.receivedMessages!) {
        var message = utf8.decode(base64Decode(receivedMessage.message!.data!));

        try {
          var decodedMessage = json.decode(message);

          // Se a mensagem de RELEASE for recebida, remove da fila e retorna
          if (decodedMessage['action'] == 'RELEASE' &&
              decodedMessage['timestamp'] == acquireTimestamp &&
              decodedMessage['serverId'] == serverId) {
            logger.info('Received RELEASE message from server $serverId with timestamp $acquireTimestamp');

            // Remove a mensagem correspondente da fila
            messageQueue.removeWhere((msg) => msg['serverId'] == serverId && msg['timestamp'] == acquireTimestamp);
            logger.info('Message ($acquireTimestamp) removed from queue for server $serverId');

            return; // Retorna quando a mensagem de RELEASE é processada
          }
        } catch (e) {
          logger.severe('Error decoding message: $e');
        }

        // Confirma que a mensagem foi processada
        await pubSubClient.projects.subscriptions.acknowledge(
          AcknowledgeRequest(ackIds: [receivedMessage.ackId!]),
          subscription,
        );
      }
    }
  }
}

void main(List<String> args) async {
  var serverId = args.isNotEmpty ? args[0] : '0';

  // Configuração do Logger
  final Logger logger = Logger('Server $serverId');
  setupLogging();

  var pubSubClient = await createPubSubClient();
  var syncServer = SyncServer(
    serverId: serverId,
    pubSubClient: pubSubClient,
    topic: 'projects/tp2-bcc362/topics/sync-topic',
    subscription: 'projects/tp2-bcc362/subscriptions/sync-topic-sub',
    logger: logger,
  );

  // Define o handler do servidor Shelf para lidar com requisições
  var handler = const Pipeline().addMiddleware(logRequests()).addHandler((Request request) async {
    if (request.method == 'POST' && request.url.path == 'acquire') {
      // Envia a mensagem de ACQUIRE e aguarda até poder acessar o recurso
      final int acquireTimestamp = DateTime.now().millisecondsSinceEpoch;

      await syncServer.publishAcquireMessage(acquireTimestamp);

      // Aguarda até que o recurso possa ser acessado e depois liberado
      var accessGranted = await syncServer.listenForMessages(acquireTimestamp);

      if (accessGranted) {
        // Retorna 200 OK após receber o RELEASE e remover da fila
        return Response.ok('Resource accessed and released successfully by server $serverId');
      } else {
        return Response.internalServerError(body: 'Failed to access resource');
      }
    }
    return Response.notFound('Not Found');
  });

  // Inicializa o servidor Shelf na porta 8080
  var server = await io.serve(handler, InternetAddress.anyIPv4, 8080);
  logger.info('Sync server is running on http://${server.address.host}:${server.port}');
}
