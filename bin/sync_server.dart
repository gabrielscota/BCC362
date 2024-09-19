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

final Queue<Map<String, dynamic>> acquireQueue = Queue();
final Queue<Map<String, dynamic>> releaseQueue = Queue();

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
    final int messageId = DateTime.now().millisecondsSinceEpoch;
    completerList.add({messageId: completer});

    final message = json.encode({
      'clusterSyncId': clusterSyncId,
      'primitive': MessagePrimitive.ACQUIRE.name,
      'messageId': messageId,
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
  Future<void> publishReleaseMessage(int messageId) async {
    var message = json.encode({
      'clusterSyncId': clusterSyncId,
      'primitive': MessagePrimitive.RELEASE.name,
      'messageId': messageId,
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

            final alreadyInQueue = acquireQueue.any((element) =>
                element['clusterSyncId'] == decodedMessage['clusterSyncId'] &&
                element['messageId'] == decodedMessage['messageId']);
            if (!alreadyInQueue) {
              // Adiciona a mensagem à fila de mensagens ACQUIRE)
              acquireQueue.add(decodedMessage);
              logger.info('Added message (${decodedMessage['messageId']}) to queue: $acquireQueue');

              await pubSubClient.projects.subscriptions.acknowledge(
                AcknowledgeRequest(ackIds: [receivedMessage.ackId!]),
                subscription,
              );
            }
          } else if (decodedMessage['primitive'] == MessagePrimitive.RELEASE.name) {
            logger.info('Received RELEASE message: $decodedMessage');

            // Adiciona a mensagem à fila de mensagens RELEASE
            releaseQueue.add(decodedMessage);
            logger.info('Added message (${decodedMessage['messageId']}) to queue: $releaseQueue');

            await pubSubClient.projects.subscriptions.acknowledge(
              AcknowledgeRequest(ackIds: [receivedMessage.ackId!]),
              subscription,
            );

            if (acquireQueue.isNotEmpty && acquireQueue.first['messageId'] == decodedMessage['messageId']) {
              lastProcessedMessageId = -1;
            }
          }
        } catch (e) {
          logger.severe('Error decoding message: $e');
        }
      }
    }
  }

  int lastProcessedMessageId = -1;

  Future<void> processMessages() async {
    while (true) {
      await Future.delayed(Duration(milliseconds: 100));

      if (acquireQueue.isNotEmpty && releaseQueue.isNotEmpty) {
        final acquireMessage = acquireQueue.first;
        final releaseMessage = releaseQueue.first;

        if (acquireMessage['messageId'] == releaseMessage['messageId']) {
          acquireQueue.removeFirst();
          releaseQueue.removeFirst();
        }
      } else if (acquireQueue.isNotEmpty && releaseQueue.isEmpty) {
        final acquireMessage = acquireQueue.first;
        final messageId = acquireMessage['messageId'];

        if (acquireMessage['clusterSyncId'] == clusterSyncId && messageId != lastProcessedMessageId) {
          await accessResource();
          await publishReleaseMessage(messageId);
          lastProcessedMessageId = messageId;
        }
      }
    }
  }

  // Simula o acesso ao recurso
  Future<void> accessResource() async {
    logger.info('Sync $clusterSyncId accessing the critical section...');
    int criticalRegionTime = 200 + Random().nextInt(801);
    await Future.delayed(Duration(milliseconds: criticalRegionTime));
    logger.info('Sync $clusterSyncId left the critical section.');
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

  final Completer<bool> completer = Completer();
  await syncServer.publishAcquireMessage(completer);

  final Completer<bool> completer2 = Completer();
  await syncServer.publishAcquireMessage(completer2);

  final Completer<bool> completer3 = Completer();
  await syncServer.publishAcquireMessage(completer3);

  // Inicia o processamento de mensagens
  syncServer.processMessages();
}
