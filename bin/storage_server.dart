import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as io;

// Classe do servidor de armazenamento
class StorageServer {
  final int id;
  bool isPrimary;
  String primaryAddress;
  List<String> backups = [];
  bool isRunning = true; // Controla se o servidor está ativo ou inativo
  static const String FAILURE_TRIGGER_KEY = "trigger_failure"; // Chave para simular falha

  StorageServer(this.id, this.isPrimary, this.primaryAddress, this.backups);

  // Função para realizar a replicação nos backups
  Future<void> replicateToBackups(String content) async {
    for (var backup in backups) {
      try {
        var client = HttpClient();
        var request =
            await client.postUrl(Uri.parse('http://$backup:${int.parse(backup.split('storage').last) + 8085}/update'));
        request.headers.contentType = ContentType.json;
        request.write(jsonEncode({'data': content}));
        var response = await request.close();
        if (response.statusCode == HttpStatus.ok) {
          print('[INFO] Backup $backup atualizado com sucesso.');
        } else {
          print('[ERROR] Erro ao atualizar o backup $backup. Código: ${response.statusCode}');
        }
      } catch (e) {
        print('[ERROR] Falha ao se conectar com o backup $backup: $e');
      }
    }
  }

  // Simula uma falha forçando o servidor a "parar"
  void simulateFailure() {
    print('[FAILURE] Storage Server $id falhou! Simulação de falha iniciada.');
    exit(1); // Mata o processo simulando uma falha
  }

  // Função que será chamada nas requisições de escrita do primário
  Future<Response> _handleWrite(Request request) async {
    if (isPrimary) {
      // O primário processa a escrita
      var content = await request.readAsString();
      var data = jsonDecode(content);

      print('[INFO] Primário $id processando dados: $content');
      try {
        // Verifica se o body contém a chave para simular uma falha
        if (data.containsKey(FAILURE_TRIGGER_KEY)) {
          simulateFailure(); // Simula falha antes da replicação
        }

        await Future.delayed(Duration(seconds: 2)); // Simulação de tempo de escrita
        await replicateToBackups(content); // Replicar a escrita para os backups
        print('[INFO] Escrita e replicação concluídas no Primário $id.');
        return Response.ok('[INFO] Escrita bem-sucedida no Primário $id e replicada.');
      } catch (e) {
        print('[ERROR] Erro durante a replicação: $e');
        return Response.internalServerError(body: 'Erro durante a replicação');
      }
    } else {
      // Backup redireciona para o primário
      return _forwardToPrimary(request);
    }
  }

  // Função para redirecionar requisições de escrita para o primário
  Future<Response> _forwardToPrimary(Request request) async {
    try {
      var client = HttpClient();
      var content = await request.readAsString();
      var data = jsonDecode(content);

      // Verifica se o body contém a chave para simular uma falha no backup
      if (data.containsKey(FAILURE_TRIGGER_KEY)) {
        simulateFailure(); // Simula falha no backup
      }

      var req = await client
          .postUrl(Uri.parse('http://$primaryAddress:${int.parse(primaryAddress.split('storage').last) + 8085}/write'));
      req.headers.contentType = ContentType.json;
      req.write(content);
      var response = await req.close();

      if (response.statusCode == HttpStatus.ok) {
        print('[INFO] Backup $id redirecionou a escrita para o primário $primaryAddress com sucesso.');
        return Response.ok('[INFO] Escrita redirecionada para o primário.');
      } else {
        print('[ERROR] Erro ao redirecionar a escrita para o primário $primaryAddress.');
        return Response.internalServerError(body: 'Erro ao redirecionar para o primário.');
      }
    } catch (e) {
      print('[ERROR] Falha ao se conectar com o primário $primaryAddress: $e');
      return Response.internalServerError(body: 'Erro ao se conectar com o primário.');
    }
  }

  // Função que será chamada nas atualizações recebidas dos backups
  Future<Response> _handleUpdate(Request request) async {
    var content = await request.readAsString();
    print('[INFO] Backup $id recebendo atualização: $content');
    await Future.delayed(Duration(seconds: 1)); // Simulação de tempo de escrita no backup
    return Response.ok('[INFO] Backup $id atualizado.');
  }

  // Função para verificar se o primário está vivo (heartbeat)
  Future<void> _checkPrimaryStatus() async {
    while (isRunning) {
      if (!isPrimary) {
        try {
          var client = HttpClient();
          var request = await client.getUrl(
              Uri.parse('http://$primaryAddress:${int.parse(primaryAddress.split('storage').last) + 8085}/ping'));
          var response = await request.close();
          if (response.statusCode != HttpStatus.ok) {
            print('[ERROR] Falha ao conectar ao primário $primaryAddress. Iniciando eleição...');
            _startElection();
          }
        } catch (e) {
          print('[ERROR] Primário $primaryAddress não respondeu. Iniciando eleição...');
          _startElection();
        }
      }
      await Future.delayed(Duration(seconds: 5)); // Heartbeat a cada 5 segundos
    }
  }

  // Eleição de um novo primário
  void _startElection() async {
    if (backups.isNotEmpty) {
      print('[INFO] Backup $id se tornando o novo primário.');
      isPrimary = true;
      primaryAddress = 'storage$id'; // Atualiza o endereço do primário
      backups.remove('storage$id'); // Remove este servidor da lista de backups

      // Notificar os outros backups que este servidor agora é o primário
      await notifyBackups();
    }
  }

  // Notifica os backups que um novo primário foi eleito
  Future<void> notifyBackups() async {
    for (var backup in backups) {
      try {
        var client = HttpClient();
        var request = await client
            .postUrl(Uri.parse('http://$backup:${int.parse(backup.split('storage').last) + 8085}/primaryChange'));
        request.headers.contentType = ContentType.json;
        request.write(jsonEncode({'newPrimary': 'storage$id'}));
        var response = await request.close();
        if (response.statusCode == HttpStatus.ok) {
          print('[INFO] Backup $backup notificado do novo primário storage$id.');
        } else {
          print('[ERROR] Erro ao notificar o backup $backup. Código: ${response.statusCode}');
        }
      } catch (e) {
        print('[ERROR] Falha ao se conectar com o backup $backup: $e');
      }
    }
  }

  // Função para lidar com notificações de mudança de primário
  Future<Response> _handlePrimaryChange(Request request) async {
    var content = await request.readAsString();
    var data = jsonDecode(content);
    primaryAddress = data['newPrimary'];
    print('[INFO] Backup $id recebeu notificação: novo primário é $primaryAddress.');
    return Response.ok('Primário atualizado.');
  }

  // Função para criar um middleware de logging customizado
  Middleware customLogRequests() {
    return (Handler innerHandler) {
      return (Request request) async {
        // Verifica se a requisição é um ping
        if (request.method == 'GET' && request.url.path == 'ping') {
          // Não loga a requisição de ping
          return await innerHandler(request);
        }

        // Caso contrário, faz o log normalmente
        final startTime = DateTime.now();
        final response = await innerHandler(request);
        final duration = DateTime.now().difference(startTime);

        // Log customizado para requisições não ping
        print(
            '${startTime.toIso8601String()} ${request.method} ${request.requestedUri.path} [${response.statusCode}] (${duration.inMilliseconds} ms)');

        return response;
      };
    };
  }

  // Iniciar o servidor com Shelf
  void start() async {
    var handler = const Pipeline().addMiddleware(customLogRequests()).addHandler((Request request) {
      if (request.method == 'POST' && request.url.path == 'write') {
        return _handleWrite(request);
      } else if (request.method == 'POST' && request.url.path == 'update') {
        return _handleUpdate(request);
      } else if (request.method == 'POST' && request.url.path == 'primaryChange') {
        return _handlePrimaryChange(request);
      } else if (request.method == 'GET' && request.url.path == 'ping') {
        return Response.ok('Pong');
      } else {
        return Response.notFound('Not found');
      }
    });

    // Bind do servidor na porta 8080 + id para garantir portas diferentes
    var server = await io.serve(handler, InternetAddress.anyIPv4, 8085 + id);
    print('Storage Server $id escutando em http://${server.address.host}:${server.port}');

    // Iniciar o monitoramento de heartbeat
    _checkPrimaryStatus();
  }
}

void main(List<String> args) {
  if (args.isEmpty) {
    print('[ERROR] Forneça o ID do servidor e se é primário ou não');
    return;
  }

  var serverId = int.parse(args[0]);
  var isPrimary = args[1] == 'true'; // O segundo argumento define se é primário
  var primaryAddress = 'storage0'; // Definir qual storage é o primário

  var backups = ['storage1', 'storage2']; // Lista de backups

  // Se o servidor for um backup, removê-lo da lista de backups
  backups.remove('storage$serverId');

  var server = StorageServer(serverId, isPrimary, primaryAddress, backups);
  server.start();
}
