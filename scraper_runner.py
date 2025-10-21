import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional

from main import Place, save_places_to_csv_streaming, scrape_places


@dataclass
class ScraperCallbacks:
    progress: Optional[Callable[[Dict[str, Any]], None]] = None
    metrics: Optional[Callable[[Dict[str, Any]], None]] = None
    record_batch: Optional[Callable[[Dict[str, Any]], None]] = None


@dataclass
class ScraperJobConfig:
    search_query: str
    total_results: int
    output_file: str
    append_mode: bool = False
    ultra_fast_append: bool = False


@dataclass
class ScraperRunResult:
    success: bool
    elapsed_seconds: float
    error: Optional[Exception] = None


class InProcessScraperRunner:
    def __init__(self) -> None:
        self._last_total_scraped: Optional[int] = None

    def run(
        self,
        config: ScraperJobConfig,
        callbacks: Optional[ScraperCallbacks] = None,
    ) -> ScraperRunResult:
        callbacks = callbacks or ScraperCallbacks()
        start_time = time.time()

        def handle_progress(payload: Dict[str, Any]) -> None:
            if 'current' in payload and payload['current'] is not None:
                try:
                    self._last_total_scraped = int(payload['current'])
                except (TypeError, ValueError):
                    self._last_total_scraped = payload.get('current')
            if callbacks.progress:
                callbacks.progress(payload)

        def handle_metrics(payload: Dict[str, Any]) -> None:
            if callbacks.metrics:
                callbacks.metrics(payload)

        def handle_record(payload: Dict[str, Any]) -> None:
            total_scraped = payload.get('total_scraped')
            if total_scraped is not None:
                try:
                    self._last_total_scraped = int(total_scraped)
                except (TypeError, ValueError):
                    self._last_total_scraped = total_scraped
            if callbacks.record_batch:
                callbacks.record_batch(payload)

        try:
            leftover: List[Place] = scrape_places(
                config.search_query,
                config.total_results,
                output_path=config.output_file,
                ultra_fast_append=config.ultra_fast_append or config.append_mode,
                progress_callback=handle_progress,
                metrics_callback=handle_metrics,
                record_callback=handle_record,
            )

            if leftover:
                save_places_to_csv_streaming(
                    leftover,
                    config.output_file,
                    append=config.append_mode or config.ultra_fast_append,
                )
                if callbacks.record_batch:
                    total_scraped = self._last_total_scraped
                    if total_scraped is None:
                        total_scraped = len(leftover)
                        self._last_total_scraped = total_scraped
                    record_payload = {
                        'records': [asdict(place) for place in leftover],
                        'output_file': config.output_file,
                        'timestamp': time.time(),
                        'total_scraped': total_scraped,
                        'target': config.total_results,
                    }
                    callbacks.record_batch(record_payload)

            elapsed = time.time() - start_time
            return ScraperRunResult(success=True, elapsed_seconds=elapsed)
        except Exception as exc:  # pragma: no cover - defensive logging
            logging.exception("In-process scraper runner failed: %s", exc)
            elapsed = time.time() - start_time
            return ScraperRunResult(success=False, elapsed_seconds=elapsed, error=exc)
