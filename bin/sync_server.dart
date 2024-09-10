import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:googleapis/pubsub/v1.dart' as pubsub;
import 'package:googleapis_auth/auth_io.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as io;
import 'package:shelf_router/shelf_router.dart';

Future<pubsub.PubsubApi> createPubSubClient() async {
  var accountCredentials = ServiceAccountCredentials.fromJson(File('service-account.json').readAsStringSync());

  var scopes = [pubsub.PubsubApi.cloudPlatformScope];

  var authClient = await clientViaServiceAccount(accountCredentials, scopes);
  return pubsub.PubsubApi(authClient);
}

// Função para enviar ACQUIRE ou RELEASE
Future<void> sendMessage(String syncId, String messageType) async {
  var pubSubClient = await createPubSubClient();
  var topicName = 'projects/tp2-bcc362/topics/sync-topic';

  try {
    var publishRequest = pubsub.PublishRequest.fromJson({
      'messages': [
        {
          'data': base64Encode(utf8.encode(messageType)), // "ACQUIRE" ou "RELEASE"
          'attributes': {'syncId': syncId}
        }
      ]
    });

    await pubSubClient.projects.topics.publish(publishRequest, topicName);
    print('$syncId: Mensagem $messageType enviada ao Pub/Sub');
  } catch (e) {
    print("Erro ao enviar $messageType: $e");
  }
}

// Função que escuta o Pub/Sub para receber "GRANTED"
Future<void> listenForGranted(String syncId) async {
  var pubSubClient = await createPubSubClient();
  var subscriptionName = 'projects/tp2-bcc362/subscriptions/sync-$syncId';

  try {
    var pullRequest = pubsub.PullRequest()
      ..maxMessages = 1
      ..returnImmediately = false;

    while (true) {
      print('$syncId: Aguardando GRANTED do coordenador...');
      var response = await pubSubClient.projects.subscriptions.pull(pullRequest, subscriptionName);
      if (response.receivedMessages != null && response.receivedMessages!.isNotEmpty) {
        var message = response.receivedMessages!.first.message;
        var messageData = utf8.decode(base64Decode(message!.data!));
        var receivedSyncId = message.attributes?['syncId'];

        if (messageData.contains('GRANTED') && receivedSyncId == syncId) {
          print('$syncId: Recebeu GRANTED, acessando o recurso.');

          // Sorteia um numero de 200 a 1000 milisegundos para ficar na região crítica
          final Random random = Random();
          int criticalRegionSleepTime = 200 + random.nextInt(801);
          await Future.delayed(Duration(milliseconds: criticalRegionSleepTime));

          print('$syncId: Recurso liberado.');
          await sendMessage(syncId, 'RELEASE'); // Envia RELEASE após uso do recurso

          // Confirmação do processamento da mensagem
          var ackIds =
              response.receivedMessages!.map((m) => m.ackId).where((ackId) => ackId != null).cast<String>().toList();

          await pubSubClient.projects.subscriptions.acknowledge(
            pubsub.AcknowledgeRequest(ackIds: ackIds),
            subscriptionName,
          );
          break; // Sai do loop após liberar o recurso
        } else {
          // Confirma a mensagem que não era destinada a este sync
          var ackIds =
              response.receivedMessages!.map((m) => m.ackId).where((ackId) => ackId != null).cast<String>().toList();

          await pubSubClient.projects.subscriptions.acknowledge(
            pubsub.AcknowledgeRequest(ackIds: ackIds),
            subscriptionName,
          );
        }
      }
    }
  } catch (e) {
    print("Erro ao conectar ao Pub/Sub: $e");
  }
}

void main(List<String> args) async {
  final syncId = args.isNotEmpty ? args[0] : '0'; // Identificador do servidor

  var app = Router();

  // Rota para o POST do Flutter
  app.post('/sync', (Request request) async {
    print('Requisição de POST recebida no $syncId');
    // Envia ACQUIRE ao coordenador
    await sendMessage(syncId, 'ACQUIRE');
    // Escuta o GRANTED
    await listenForGranted(syncId);
    return Response.ok('GRANTED');
  });

  // Iniciar o servidor
  var server = await io.serve(app.call, InternetAddress.anyIPv4, 8080);
  print('Servidor $syncId rodando em http://localhost:${server.port}');
}
