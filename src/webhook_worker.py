"""Fault-tolerant async webhook worker with exponential backoff and dead-letter queue."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .payload_transformer import normalize_lead_payload

logger = logging.getLogger("webhook_worker")


@dataclass
class WorkerMetrics:
    """Tracks execution metrics across batch webhook deliveries."""

    total_received: int = 0
    succeeded: int = 0
    retried_requests: int = 0
    rejected_schema: int = 0
    dead_lettered: int = 0
    latencies_ms: List[float] = field(default_factory=list)

    @property
    def avg_latency_ms(self) -> float:
        return round(sum(self.latencies_ms) / len(self.latencies_ms), 2) if self.latencies_ms else 0.0

    @property
    def delivery_rate_pct(self) -> float:
        valid = self.total_received - self.rejected_schema
        return round((self.succeeded / valid) * 100, 2) if valid > 0 else 100.0


class AsyncWebhookWorker:
    """Processes inbound webhook events with bounded concurrency and 429/503 retry recovery."""

    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        target_url: str,
        api_token: str,
        max_concurrency: int = 10,
        max_retries: int = 4,
        base_backoff_sec: float = 0.5,
        dlq_dir: str = "/opt/automation-worker/dead_letter_queue",
    ) -> None:
        self.target_url = target_url
        self.api_token = api_token
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.max_retries = max_retries
        self.base_backoff_sec = base_backoff_sec
        self.dlq_path = Path(dlq_dir)
        self.metrics = WorkerMetrics()

    async def deliver_with_retry(
        self, client: httpx.AsyncClient, record: Dict[str, Any]
    ) -> bool:
        """Send normalized payload to downstream REST API with exponential backoff + jitter."""
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "X-Idempotency-Key": record["lead_id"],
        }

        for attempt in range(1, self.max_retries + 1):
            start = time.perf_counter()
            try:
                response = await client.post(
                    self.target_url, json=record, headers=headers, timeout=10.0
                )
                elapsed_ms = (time.perf_counter() - start) * 1000.0

                if response.status_code in (200, 201, 202):
                    self.metrics.succeeded += 1
                    self.metrics.latencies_ms.append(elapsed_ms)
                    return True

                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    self.metrics.retried_requests += 1
                    retry_after = response.headers.get("Retry-After")
                    delay = (
                        float(retry_after)
                        if retry_after and retry_after.isdigit()
                        else self.base_backoff_sec * (2 ** (attempt - 1)) + random.uniform(0.05, 0.25)
                    )
                    logger.warning(
                        "HTTP %s for lead_id=%s (attempt %d/%d). Retrying in %.2fs",
                        response.status_code,
                        record["lead_id"],
                        attempt,
                        self.max_retries,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                logger.error("Non-retryable HTTP %s for lead_id=%s", response.status_code, record["lead_id"])
                break

            except httpx.RequestError as exc:
                self.metrics.retried_requests += 1
                delay = self.base_backoff_sec * (2 ** (attempt - 1))
                logger.warning("Network error (%s) on attempt %d: %s", type(exc).__name__, attempt, exc)
                await asyncio.sleep(delay)

        self._write_dead_letter(record)
        self.metrics.dead_lettered += 1
        return False

    def _write_dead_letter(self, record: Dict[str, Any]) -> None:
        """Persist exhausted payloads to disk so zero events are ever lost."""
        self.dlq_path.mkdir(parents=True, exist_ok=True)
        out_file = self.dlq_path / f"{record['lead_id']}.json"
        out_file.write_text(json.dumps(record, indent=2), encoding="utf-8")

    async def process_batch(
        self, raw_events: List[Dict[str, Any]], transport: Optional[httpx.AsyncBaseTransport] = None
    ) -> WorkerMetrics:
        """Validate, normalize, and concurrently dispatch a batch of webhook payloads."""
        async with httpx.AsyncClient(transport=transport) as client:
            tasks = []
            for raw in raw_events:
                self.metrics.total_received += 1
                normalized = normalize_lead_payload(raw)
                if not normalized["is_valid"]:
                    self.metrics.rejected_schema += 1
                    continue
                tasks.append(self._bounded_deliver(client, normalized))

            await asyncio.gather(*tasks)
        return self.metrics

    async def _bounded_deliver(self, client: httpx.AsyncClient, record: Dict[str, Any]) -> bool:
        async with self.semaphore:
            return await self.deliver_with_retry(client, record)
