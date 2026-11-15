"""Local runnable benchmark demonstrating 429 rate-limit recovery with zero external API keys."""

import asyncio
import json
import random
import httpx

from src.webhook_worker import AsyncWebhookWorker


class MockDownstreamTransport(httpx.AsyncBaseTransport):
    """Simulates a production CRM API experiencing transient HTTP 429 rate-limit spikes."""

    def __init__(self) -> None:
        self.attempts: dict[str, int] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content.decode("utf-8"))
        lead_id = payload["lead_id"]
        self.attempts[lead_id] = self.attempts.get(lead_id, 0) + 1

        # Simulate 429 Too Many Requests on first attempt for ~12% of traffic
        if self.attempts[lead_id] == 1 and random.random() < 0.12:
            return httpx.Response(429, json={"error": "rate_limit_exceeded"}, headers={"Retry-After": "0"})

        await asyncio.sleep(0.01)
        return httpx.Response(200, json={"status": "synced", "lead_id": lead_id})


async def main() -> None:
    events = [
        {
            "event_id": f"evt_{i:04d}",
            "utm_source": "n8n_bridge",
            "contact": {
                "first_name": f"User{i}",
                "last_name": "Enterprise",
                "email": f"buyer{i}@company{i % 25}.io",
            },
        }
        for i in range(1, 251)
    ]

    worker = AsyncWebhookWorker(
        target_url="https://api.crm-gateway.internal/v2/leads",
        api_token="demo_token_sanitized",
        max_concurrency=15,
        base_backoff_sec=0.05,
    )
    metrics = await worker.process_batch(events, transport=MockDownstreamTransport())
    print(f"Processed: {metrics.total_received} | Succeeded: {metrics.succeeded} | "
          f"429 Retries Recovered: {metrics.retried_requests} | "
          f"Delivery Rate: {metrics.delivery_rate_pct}% | Avg Latency: {metrics.avg_latency_ms} ms")


if __name__ == "__main__":
    asyncio.run(main())
