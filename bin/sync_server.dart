import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:googleapis/pubsub/v1.dart';
import 'package:googleapis_auth/auth_io.dart';
import 'package:logging/logging.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as io;

Queue<Map<String, dynamic>> acquireQueue = Queue();
Queue<Map<String, dynamic>> releaseQueue = Queue();

// Configuração do Logger
void setupLogging() {
  Logger.root.level = Level.ALL;
  Logger.root.onRecord.listen((record) {
    print('[${record.loggerName}] [${record.level.name}]: ${record.message}');
  });
}

// Cria um cliente Pub/Sub com as credenciais do arquivo service-account.json
Future<PubsubApi> createPubSubClient() async {
  final ServiceAccountCredentials accountCredentials =
      ServiceAccountCredentials.fromJson(File('service-account.json').readAsStringSync());

  final List<String> scopes = [PubsubApi.cloudPlatformScope];

  final AutoRefreshingAuthClient authClient = await clientViaServiceAccount(accountCredentials, scopes);

  return PubsubApi(authClient);
}

enum MessagePrimitive { ACQUIRE, RELEASE }

class SyncServer {
  final String clusterSyncId;
  final String topic;
  final String subscription;
  final PubsubApi pubSubClient;
  final Logger logger;

  SyncServer({
    required this.clusterSyncId,
    required this.topic,
    required this.subscription,
    required this.pubSubClient,
    required this.logger,
  });

  List<Map<int, Completer<bool>>> completerList = [];

  // Publica uma mensagem de ACQUIRE ao Pub/Sub com timestamp
  Future<void> publishAcquireMessage(Completer<bool> completer) async {
    final int messageId = DateTime.now().millisecondsSinceEpoch + Random().nextInt(1000);
    completerList.add({messageId: completer});

    final message = json.encode({
      'clusterSyncId': clusterSyncId,
      'primitive': MessagePrimitive.ACQUIRE.name,
      'messageId': messageId,
    });
    await pubSubClient.projects.topics.publish(
      PublishRequest(
        messages: [
          PubsubMessage(
            data: base64Encode(utf8.encode(message)),
            orderingKey: 'acquire',
          )
        ],
      ),
      topic,
    );
    logger.info('Published ACQUIRE message: $message');
  }

  // Envia uma mensagem de RELEASE ao Pub/Sub, incluindo o timestamp original de ACQUIRE
  Future<void> publishReleaseMessage(int messageId) async {
    var message = json.encode({
      'clusterSyncId': clusterSyncId,
      'primitive': MessagePrimitive.RELEASE.name,
      'messageId': messageId,
    });
    await pubSubClient.projects.topics.publish(
      PublishRequest(
        messages: [
          PubsubMessage(
            data: base64Encode(utf8.encode(message)),
            orderingKey: 'release',
          )
        ],
      ),
      topic,
    );
    logger.info('Published RELEASE message: $message');
  }

  // Escuta mensagens do Pub/Sub e processa as de ACQUIRE e RELEASE
  Future<void> listenMessages() async {
    while (true) {
      final PullResponse response = await pubSubClient.projects.subscriptions.pull(
        PullRequest(returnImmediately: false, maxMessages: 10),
        subscription,
      );

      if (response.receivedMessages == null) {
        continue;
      }

      for (final receivedMessage in response.receivedMessages!) {
        final String message = utf8.decode(base64Decode(receivedMessage.message!.data!));

        try {
          final decodedMessage = json.decode(message);

          if (decodedMessage['primitive'] == MessagePrimitive.ACQUIRE.name) {
            logger.info('Received ACQUIRE message: $decodedMessage');

            // Adiciona a mensagem à fila de mensagens ACQUIRE)
            acquireQueue.add(decodedMessage);
            logger.info('Added message (${decodedMessage['messageId']}) to queue: $acquireQueue');

            await pubSubClient.projects.subscriptions.acknowledge(
              AcknowledgeRequest(ackIds: [receivedMessage.ackId!]),
              subscription,
            );
          } else if (decodedMessage['primitive'] == MessagePrimitive.RELEASE.name) {
            logger.info('Received RELEASE message: $decodedMessage');

            // Adiciona a mensagem à fila de mensagens RELEASE
            releaseQueue.add(decodedMessage);
            logger.info('Added message (${decodedMessage['messageId']}) to queue: $releaseQueue');

            await pubSubClient.projects.subscriptions.acknowledge(
              AcknowledgeRequest(ackIds: [receivedMessage.ackId!]),
              subscription,
            );

            if (decodedMessage['clusterSyncId'] == clusterSyncId) {
              final messageId = decodedMessage['messageId'];
              final completer = completerList.firstWhere((element) => element.containsKey(messageId)).values.first;
              completer.complete(true);
              completerList.removeWhere((element) => element.containsKey(messageId));
            }
          }
        } catch (e) {
          logger.severe('Error decoding message: $e');
        }
      }
    }
  }

  Future<void> processMessages() async {
    while (true) {
      await Future.delayed(Duration(milliseconds: 100)); // Controle de periodicidade

      if (acquireQueue.isNotEmpty) {
        final firstAcquireMessage = acquireQueue.first;

        if (firstAcquireMessage['clusterSyncId'] == clusterSyncId) {
          final messageId = firstAcquireMessage['messageId'];
          final hasReleaseMessage = releaseQueue.any((releaseMessage) => releaseMessage['messageId'] == messageId);

          if (!hasReleaseMessage) {
            logger.info('Sync $clusterSyncId está acessando o recurso...');
            await accessResource(messageId);
            await publishReleaseMessage(messageId);
            acquireQueue.removeFirst();
            logger.info('Sync $clusterSyncId liberou o recurso e removeu a mensagem da fila de ACQUIRE.');
          } else {
            logger.info('Sync $clusterSyncId detectou que o recurso já foi liberado.');
            releaseQueue.removeWhere((releaseMessage) => releaseMessage['messageId'] == messageId);
            acquireQueue.removeFirst();
            logger.info('Removida a mensagem de RELEASE da fila de RELEASE.');
          }
        } else {
          // Coleta as mensagens que precisam ser removidas em uma lista temporária
          List<int> messageIdsToRemove = [];

          // Verifica se existe um RELEASE correspondente a outro sync na fila de ACQUIRE
          for (var acquireMessage in acquireQueue) {
            final acquireMessageId = acquireMessage['messageId'];
            final acquireClusterId = acquireMessage['clusterSyncId'];

            final hasReleaseMessage =
                releaseQueue.any((releaseMessage) => releaseMessage['messageId'] == acquireMessageId);

            if (hasReleaseMessage) {
              logger.info('Sync $clusterSyncId detectou que o recurso do Sync $acquireClusterId foi liberado.');

              // Adiciona o ID da mensagem para remoção após a iteração
              messageIdsToRemove.add(acquireMessageId);
            }
          }

          // Remove as mensagens coletadas da fila de ACQUIRE e RELEASE após a iteração
          for (var messageId in messageIdsToRemove) {
            acquireQueue.removeWhere((msg) => msg['messageId'] == messageId);
            releaseQueue.removeWhere((releaseMessage) => releaseMessage['messageId'] == messageId);
            logger
                .info('Removidos ACQUIRE e RELEASE correspondentes do Sync com messageId $messageId das filas locais.');
          }
        }
      }
    }
  }

  // Simula o acesso ao recurso
  Future<void> accessResource(int messageId) async {
    logger.info('Sync $clusterSyncId e a mensagem ($messageId) está entrando na seção crítica...');
    int criticalRegionTime = 200 + Random().nextInt(801);
    await Future.delayed(Duration(milliseconds: criticalRegionTime));
    logger.info('Sync $clusterSyncId e a mensagem ($messageId) saiu da seção crítica.');
  }
}

void main(List<String> args) async {
  final String clusterSyncId = args.isNotEmpty ? args[0] : '0';

  // Configuração do Logger
  final Logger logger = Logger('Sync $clusterSyncId');
  setupLogging();

  var pubSubClient = await createPubSubClient();
  var syncServer = SyncServer(
    clusterSyncId: clusterSyncId,
    pubSubClient: pubSubClient,
    topic: 'projects/tp2-bcc362/topics/sync-topic',
    subscription: 'projects/tp2-bcc362/subscriptions/sync-topic-sub-$clusterSyncId',
    logger: logger,
  );

  // Define o handler do servidor Shelf para lidar com requisições
  var handler = const Pipeline().addMiddleware(logRequests()).addHandler((Request request) async {
    if (request.method == 'POST' && request.url.path == 'acquire') {
      final Completer<bool> completer = Completer();
      await syncServer.publishAcquireMessage(completer);

      // Aguarda até que o recurso possa ser acessado e depois liberado
      final bool result = await completer.future;

      if (result) {
        // Retorna 200 OK após receber o RELEASE e remover da fila
        return Response.ok('Resource accessed and released successfully by sync $clusterSyncId');
      } else {
        return Response.internalServerError(body: 'Failed to access resource');
      }
    }
    return Response.notFound('Not Found');
  });

  // Inicializa o servidor Shelf na porta 8080
  final HttpServer server = await io.serve(handler, InternetAddress.anyIPv4, 8080);
  logger.info('Sync server is running on http://${server.address.host}:${server.port}');

  // Inicia a escuta de mensagens do Pub/Sub
  syncServer.listenMessages();

  // Inicia o processamento de mensagens
  syncServer.processMessages();
}
