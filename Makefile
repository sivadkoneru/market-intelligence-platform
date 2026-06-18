.PHONY: default setup test lint format up down ps smoke-sb smoke-ws

default:
	task

setup:
	task setup

test:
	task test

lint:
	task lint

format:
	task format

up:
	task up

down:
	task down

ps:
	task ps

smoke-sb:
	task smoke:sb

smoke-ws:
	task smoke:ws
