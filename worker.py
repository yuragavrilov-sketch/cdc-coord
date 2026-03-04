import os
import socket

from coordinator.config import AppConfig
from coordinator.db import Database
from coordinator.logging_config import configure_logging
from coordinator.repositories import CoordinatorRepository
from coordinator.worker import BulkWorker, CDCWorker, MigrationWorker


def _make_worker_id() -> str:
    hostname = socket.gethostname()
    pid = os.getpid()
    return f"{hostname}-{pid}"


def main() -> None:
    config = AppConfig.from_env()
    configure_logging(config.log_level)

    db = Database(config.postgres_dsn)
    repository = CoordinatorRepository(db)
    repository.ensure_schema()

    worker_id = os.getenv("WORKER_ID") or _make_worker_id()

    # WORKER_TYPE controls the role of this process:
    #   bulk  — processes ROWID chunks only (run 4+ of these)
    #   cdc   — handles CDC streams for multiple tables simultaneously (run 2+ of these)
    #   mixed — legacy: bulk + CDC in one process (default, backward-compatible)
    worker_type = os.getenv("WORKER_TYPE", "mixed").lower().strip()

    if worker_type == "cdc":
        worker = CDCWorker(worker_id=worker_id, config=config, repository=repository)
    elif worker_type == "bulk":
        worker = BulkWorker(worker_id=worker_id, config=config, repository=repository)
    else:
        worker = MigrationWorker(worker_id=worker_id, config=config, repository=repository)

    worker.run()


if __name__ == "__main__":
    main()
