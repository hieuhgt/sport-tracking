.PHONY: up down logs ps build simulate stop-sim scale-consumer kafka-ui health

up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps

# Start the event simulator (sends random events to Kafka)
simulate:
	curl -s -X POST http://localhost:3001/simulate/start | jq .

stop-sim:
	curl -s -X POST http://localhost:3001/simulate/stop | jq .

# Scale consumer to N replicas (default 3)
scale-consumer:
	docker compose up -d --scale consumer=$(or $(N),3)

# Open Kafka UI in browser
kafka-ui:
	open http://localhost:8090

# Open the sports dashboard
dashboard:
	open http://localhost

# Health check all services
health:
	@echo "=== Producer ===" && curl -s http://localhost:3001/health | jq .
	@echo "=== Nginx ===" && curl -s -o /dev/null -w "%{http_code}" http://localhost && echo

# Send a manual World Cup goal event
send-wc-goal:
	curl -s -X POST http://localhost:3001/events/worldcup \
		-H "Content-Type: application/json" \
		-d '{"type":"GOAL","matchId":"wc-match-1","minute":67,"player":{"id":"p1","name":"Vinicius Jr","number":7,"teamId":"brazil"},"homeTeam":{"id":"brazil","name":"Brazil","shortName":"BRA","score":2},"awayTeam":{"id":"germany","name":"Germany","shortName":"GER","score":1}}' | jq .

# Send a manual NBA basket event
send-nba-basket:
	curl -s -X POST http://localhost:3001/events/nba \
		-H "Content-Type: application/json" \
		-d '{"type":"BASKET_3PT","gameId":"nba-game-1","matchId":"nba-game-1","quarter":3,"clock":"4:22","points":3,"player":{"id":"p10","name":"Stephen Curry","number":30,"teamId":"warriors"},"homeTeam":{"id":"lakers","name":"Los Angeles Lakers","shortName":"LAL","score":87},"awayTeam":{"id":"warriors","name":"Golden State Warriors","shortName":"GSW","score":91}}' | jq .
