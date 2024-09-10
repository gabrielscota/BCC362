import 'dart:convert';
import 'dart:io';

import 'package:googleapis/pubsub/v1.dart' as pubsub;
import 'package:googleapis_auth/auth_io.dart';

Future<pubsub.PubsubApi> createPubSubClient() async {
  var accountCredentials = ServiceAccountCredentials.fromJson(File('service-account.json').readAsStringSync());

  var scopes = [pubsub.PubsubApi.cloudPlatformScope];

  var authClient = await clientViaServiceAccount(accountCredentials, scopes);
  return pubsub.PubsubApi(authClient);
}

void main() async {
  var pubSubClient = await createPubSubClient();
  var topicName = 'projects/tp2-bcc362/topics/sync-topic';
  var subscriptionName = 'projects/tp2-bcc362/subscriptions/coordinator-subscription';

  var pendingSyncs = <String>[]; // Fila de servidores sync esperando para acessar o recurso
  var resourceInUse = false; // Indica se o recurso está sendo usado

  // Função para enviar "GRANTED" para um servidor sync
  Future<void> grantAccess(String syncId) async {
    var publishRequest = pubsub.PublishRequest.fromJson({
      'messages': [
        {
          'data': base64Encode(utf8.encode('GRANTED')),
          'attributes': {'syncId': syncId}
        }
      ]
    });

    await pubSubClient.projects.topics.publish(publishRequest, topicName);
    print('Coordenador: Enviou GRANTED para $syncId');
  }

  // Iniciar o loop de escuta
  while (true) {
    var pullRequest = pubsub.PullRequest()
      ..maxMessages = 10
      ..returnImmediately = false;

    var response = await pubSubClient.projects.subscriptions.pull(pullRequest, subscriptionName);

    if (response.receivedMessages != null && response.receivedMessages!.isNotEmpty) {
      var ackIds = <String>[];

      for (var receivedMessage in response.receivedMessages!) {
        var message = receivedMessage.message;
        var messageData = utf8.decode(base64Decode(message!.data!));
        var syncId = message.attributes?['syncId'];

        if (messageData.contains('ACQUIRE')) {
          print('Coordenador: Recebeu ACQUIRE de $syncId');
          pendingSyncs.add(syncId!);
        } else if (messageData.contains('RELEASE')) {
          print('Coordenador: Recebeu RELEASE de $syncId');
          resourceInUse = false;
        }

        ackIds.add(receivedMessage.ackId!);
      }

      // Confirmação das mensagens processadas
      await pubSubClient.projects.subscriptions.acknowledge(
        pubsub.AcknowledgeRequest(ackIds: ackIds),
        subscriptionName,
      );

      // Conceder acesso se o recurso estiver livre e houver pedidos pendentes
      if (!resourceInUse && pendingSyncs.isNotEmpty) {
        var nextSyncId = pendingSyncs.removeAt(0);
        await grantAccess(nextSyncId);
        resourceInUse = true;
      }
    }
  }
}
