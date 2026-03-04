from __future__ import annotations

import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

_STATUS_LABELS: dict[str, str] = {
    "pending": "ожидание",
    "bulk_running": "bulk-загрузка",
    "bulk_loading": "bulk-загрузка",
    "bulk_done": "bulk завершён",
    "cdc_accumulating": "накопление CDC",
    "cdc_catchup": "CDC догоняет",
    "cdc_streaming": "CDC стриминг",
    "completed": "завершён",
}

_MODE_LABELS: dict[str, str] = {
    "cdc": "CDC",
    "static": "Static",
    "hybrid": "Hybrid",
}

_CONNECTOR_LABELS: dict[str, str] = {
    "RUNNING": "работает",
    "FAILED": "УПАЛ",
    "PAUSED": "на паузе",
    "MISSING": "не найден",
    "UNKNOWN": "неизвестно",
}


def _sl(status: str) -> str:
    return _STATUS_LABELS.get(status, status)


def _ml(mode: str) -> str:
    return _MODE_LABELS.get(mode, mode)


def _cl(connector_status: str) -> str:
    return _CONNECTOR_LABELS.get(connector_status, connector_status)


def _format_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}с"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}мин {s}с"
    h, m = divmod(m, 60)
    return f"{h}ч {m}мин"


class VKTeamsNotifier:
    """Sends notifications to a VK Teams chat via Bot API.

    All send operations are fire-and-forget: failures are logged but never raised.
    Supports on-premise VK Teams installations via the api_url parameter.
    """

    def __init__(self, token: str, chat_id: str, api_url: str) -> None:
        self._token = token
        self._chat_id = chat_id
        self._api_url = api_url.rstrip("/")

    def send(self, text: str) -> None:
        try:
            self._post(text)
        except Exception:
            logger.exception("VK Teams notification failed")

    def _post(self, text: str) -> None:
        payload = urllib.parse.urlencode({
            "token": self._token,
            "chatId": self._chat_id,
            "text": text,
        }).encode()
        req = urllib.request.Request(
            f"{self._api_url}/messages/sendText",
            data=payload,
            method="POST",
        )
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 201):
                body = resp.read().decode("utf-8", errors="replace")
                logger.warning("VK Teams API returned %d: %s", resp.status, body)

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def notify_job_added(
        self,
        *,
        table_name: str,
        mode: str,
        chunk_count: int,
        job_id: str,
        child_job_id: str | None = None,
        description: str | None = None,
    ) -> None:
        """Таблица добавлена на миграцию."""
        lines = [
            f"[Миграция] Добавлена таблица: {table_name}",
            f"Режим: {_ml(mode)}  |  Чанков: {chunk_count}",
            f"Job ID: {job_id}",
        ]
        if child_job_id:
            lines.append(f"Child Job (static): {child_job_id}")
        if description:
            lines.append(f"Описание: {description}")
        self.send("\n".join(lines))

    def notify_phase_change(
        self,
        *,
        table_name: str,
        job_id: str,
        from_status: str,
        to_status: str,
        mode: str,
        parent_job_id: str | None = None,
    ) -> None:
        """Смена фазы для JOB/таблицы."""
        lines = [
            f"[Миграция] Смена фазы: {table_name}",
            f"Режим: {_ml(mode)}  |  {_sl(from_status)} → {_sl(to_status)}",
            f"Job ID: {job_id}",
        ]
        if parent_job_id:
            lines.append(f"Parent CDC Job: {parent_job_id}")
        self.send("\n".join(lines))

    def notify_hybrid_child_started(
        self,
        *,
        table_name: str,
        child_job_id: str,
        parent_job_id: str,
        chunk_count: int,
    ) -> None:
        """Hybrid: child static job активирован (parent перешёл в bulk_done)."""
        self.send(
            f"[Миграция] Hybrid — запущен static child: {table_name}\n"
            f"Исторические данные готовы к загрузке  |  Чанков: {chunk_count}\n"
            f"Child Job: {child_job_id}\n"
            f"Parent CDC Job: {parent_job_id}"
        )

    def notify_migration_completed(
        self,
        *,
        table_name: str,
        job_id: str,
        mode: str,
        total_rows: int,
        duration_seconds: float | None,
        parent_job_id: str | None = None,
    ) -> None:
        """Миграция таблицы завершена (с итогами)."""
        duration_str = _format_duration(duration_seconds) if duration_seconds else "—"
        label = "[Миграция] Завершена"
        if parent_job_id:
            label = "[Миграция] Hybrid child завершён"
        lines = [
            f"{label}: {table_name}",
            f"Режим: {_ml(mode)}  |  Строк: {total_rows:,}  |  Время: {duration_str}",
            f"Job ID: {job_id}",
        ]
        if parent_job_id:
            lines.append(f"Parent CDC Job: {parent_job_id}")
        self.send("\n".join(lines))

    # ------------------------------------------------------------------
    # Alerts
    # ------------------------------------------------------------------

    def notify_connector_status_change(
        self,
        *,
        table_name: str,
        job_id: str,
        connector: str,
        from_status: str,
        to_status: str,
    ) -> None:
        """Коннектор Debezium сменил статус (упал, восстановился и т.п.)."""
        bad = to_status in ("FAILED", "MISSING", "PAUSED")
        prefix = "[Миграция] КОННЕКТОР УПАЛ" if bad else "[Миграция] Коннектор восстановлен"
        self.send(
            f"{prefix}: {table_name}\n"
            f"Статус: {_cl(from_status)} → {_cl(to_status)}\n"
            f"Коннектор: {connector}\n"
            f"Job ID: {job_id}"
        )

    def notify_cdc_ownership_lost(
        self,
        *,
        table_name: str,
        job_id: str,
    ) -> None:
        """CDC воркер не отвечал — ownership сброшен, CDC прервана."""
        self.send(
            f"[Миграция] CDC прервана: {table_name}\n"
            f"Воркер не отвечал, ownership сброшен. Job вернётся в bulk_done.\n"
            f"Job ID: {job_id}"
        )

    def notify_stale_chunks_reclaimed(
        self,
        items: list[dict[str, Any]],
    ) -> None:
        """Зависшие чанки сброшены в pending (воркер завис / упал)."""
        lines = ["[Миграция] Зависшие чанки сброшены в pending:"]
        for item in items:
            lines.append(f"  • {item['table_name']} — {item['count']} чанков")
        self.send("\n".join(lines))

    def notify_error(
        self,
        *,
        table_name: str,
        job_id: str,
        message: str,
    ) -> None:
        """Ошибка при обработке чанков."""
        self.send(
            f"[Миграция] ОШИБКА: {table_name}\n"
            f"Job ID: {job_id}\n"
            f"Ошибка: {message}"
        )

    # ------------------------------------------------------------------
    # Progress
    # ------------------------------------------------------------------

    def notify_bulk_milestone(
        self,
        *,
        table_name: str,
        job_id: str,
        mode: str,
        percent: int,
        chunks_done: int,
        chunks_total: int,
        parent_job_id: str | None = None,
    ) -> None:
        """Milestone прогресса bulk-загрузки (25 / 50 / 75 %)."""
        label = "Прогресс"
        if parent_job_id:
            label = "Прогресс (hybrid child)"
        lines = [
            f"[Миграция] {label} {percent}%: {table_name}",
            f"Режим: {_ml(mode)}  |  Чанков: {chunks_done}/{chunks_total}",
            f"Job ID: {job_id}",
        ]
        if parent_job_id:
            lines.append(f"Parent CDC Job: {parent_job_id}")
        self.send("\n".join(lines))

    # ------------------------------------------------------------------
    # Status digest
    # ------------------------------------------------------------------

    def notify_status_report(self, jobs: list[dict[str, Any]]) -> None:
        """Периодический дайджест по всем активным задачам.

        Каждый элемент jobs может содержать:
          table_name, mode, status, chunks_done, chunks_total,
          child_status (optional), child_chunks_done, child_chunks_total
        """
        if not jobs:
            self.send("[Миграция] Нет активных задач")
            return
        lines = [f"[Миграция] Статус ({len(jobs)} задач(а)):"]
        for j in jobs:
            chunks_done = j.get("chunks_done", 0)
            chunks_total = j.get("chunks_total", 0)
            chunk_info = f"{chunks_done}/{chunks_total}" if chunks_total else "—"
            line = f"  • {j['table_name']} [{_ml(j['mode'])}] {_sl(j['status'])}  чанков: {chunk_info}"
            if j.get("child_status"):
                cdone = j.get("child_chunks_done", 0)
                ctotal = j.get("child_chunks_total", 0)
                line += f"  | static child: {_sl(j['child_status'])} {cdone}/{ctotal}"
            lines.append(line)
        self.send("\n".join(lines))
