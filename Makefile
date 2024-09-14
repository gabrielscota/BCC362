# Nome do projeto (opcional, para usar em alguns comandos)
PROJECT_NAME=docker_sync_project

# Comando para parar e remover containers, volumes e imagens
down:
	docker-compose down --volumes --rmi all

# Comando para remover containers parados
clean_containers:
	docker container prune -f

# Comando para remover todas as imagens (opcional)
clean_images:
	docker rmi -f $$(docker images -a -q)

# Comando para forçar reconstrução sem cache
build:
	docker-compose build --no-cache

# Comando para subir os containers
up:
	docker-compose up

# Comando para parar, limpar containers e volumes, e reconstruir sem cache
rebuild: down clean_containers build up

# Comando para limpar tudo (containers, volumes, imagens) e reconstruir sem cache
full_clean_rebuild: clean_images rebuild

# Comando para parar e remover todos os containers e volumes
full_clean:
	docker-compose down --volumes --rmi all && docker container prune -f && docker rmi -f $$(docker images -a -q)

