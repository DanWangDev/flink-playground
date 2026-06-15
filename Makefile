.PHONY: help cluster-start cluster-stop cluster-status cluster-logs
.PHONY: build test test-coverage lint
.PHONY: exercise-% exercise-local-%

# Default target
help:
	@echo "Flink Playground — Makefile targets"
	@echo ""
	@echo "Cluster:"
	@echo "  make cluster-start       Start Flink cluster (Docker Compose)"
	@echo "  make cluster-stop        Stop Flink cluster"
	@echo "  make cluster-status      Show cluster health"
	@echo "  make cluster-logs        Show cluster logs"
	@echo ""
	@echo "Build:"
	@echo "  make build               Build fat JAR"
	@echo "  make test                Run MiniCluster tests (no Docker)"
	@echo "  make test-coverage       Run tests with coverage report"
	@echo "  make lint                Compile checks"
	@echo ""
	@echo "Exercises (Docker cluster):"
	@echo "  make exercise-01         Run exercise 01 on Docker cluster"
	@echo "  make exercise-02         Run exercise 02 on Docker cluster"
	@echo "  ...                      (through exercise-14)"
	@echo ""
	@echo "Exercises (local MiniCluster, no Docker):"
	@echo "  make exercise-local-01   Run exercise 01 with MiniCluster"
	@echo "  ...                      (through exercise-local-14)"

# ── Cluster lifecycle ──

cluster-start:
	docker compose up -d
	@echo "Waiting for Flink jobmanager..."
	@for i in $(seq 1 30); do \
		if curl -s http://localhost:8081/overview | grep -q '"taskmanagers"'; then \
			echo "Flink ready after $$i seconds"; \
			break; \
		fi; \
		sleep 2; \
	done
	@curl -s http://localhost:8081/overview | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8081/overview

cluster-stop:
	docker compose down

cluster-status:
	@curl -s http://localhost:8081/overview | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8081/overview
	@echo ""
	@curl -s http://localhost:8081/jobs | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8081/jobs

cluster-logs:
	docker compose logs --tail=100

# ── Build ──

build:
	./mvnw package -DskipTests -B

# ── Tests ──

test:
	./mvnw test -B

test-coverage:
	./mvnw test jacoco:report -B

lint:
	./mvnw compile -B

# ── Exercises (Docker Compose cluster) ──

exercise-%:
	@echo "Building JAR..."
	./mvnw package -DskipTests -B -q
	@echo "Running exercise $* on Flink cluster..."
	docker compose exec -T jobmanager \
		flink run -c playground.Main \
		/opt/flink/usrlib/flink-playground-1.0.0.jar \
		--exercise $* --no-step

# ── Exercises (local MiniCluster, no Docker) ──

exercise-local-%:
	./mvnw compile exec:java -Dexec.mainClass="playground.Main" \
		-Dexec.args="--exercise $* --local --no-step" -q
