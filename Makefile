.PHONY: setup up up-d up-b down exec composer composer-install composer-update test

setup:
	@test -f MakefileCustom || cp MakefileCustom.dist MakefileCustom

-include .env

PROJECT_NAME ?= $(shell grep -m 1 'PROJECT_NAME' .env | cut -d '=' -f2-)

DOCKER_COMPOSE = docker compose
PHP_BASH = docker exec -it php_$(PROJECT_NAME) /bin/bash
DOCKER_EXEC = $(PHP_BASH) -c

up: setup
	$(DOCKER_COMPOSE) up

up-d: setup
	$(DOCKER_COMPOSE) up -d

up-b: setup
	$(DOCKER_COMPOSE) up -d --build

down: setup
	$(DOCKER_COMPOSE) down --remove-orphans

exec: setup
	$(DOCKER_EXEC) "echo -e '\033[32m'; /bin/bash"

composer: setup
	$(DOCKER_EXEC) "composer $(CMD)"

composer-install: CMD = install
composer-install: composer

composer-update: CMD = update
composer-update: composer

composer-i: composer-install
composer-u: composer-update

-include MakefileCustom
