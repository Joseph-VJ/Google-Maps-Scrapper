import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Tuple

from scraper_runner import InProcessScraperRunner, ScraperCallbacks, ScraperJobConfig


class RateLimitExceeded(Exception):
    """Raised when job submission exceeds the configured rate limit."""


class JobNotFound(Exception):
    """Raised when a requested job does not exist."""


@dataclass
class ScrapingJobState:
    job_id: str
    search_query: str
    total_results: int
    output_file: str
    append_mode: bool
    fast_append: bool
    status: str = "pending"
    progress: float = 0.0
    result_count: int = 0
    error_message: Optional[str] = None
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    throughput_per_minute: float = 0.0
    eta_seconds: Optional[float] = None
    elapsed_seconds: float = 0.0
    last_update: Optional[float] = None
    progress_samples: Deque[Tuple[float, int]] = field(default_factory=deque)
    recent_records: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=25))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "search_query": self.search_query,
            "total_results": self.total_results,
            "output_file": self.output_file,
            "append_mode": self.append_mode,
            "fast_append": self.fast_append,
            "status": self.status,
            "progress": self.progress,
            "result_count": self.result_count,
            "error_message": self.error_message,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "throughput_per_minute": self.throughput_per_minute,
            "eta_seconds": self.eta_seconds,
            "elapsed_seconds": self.elapsed_seconds,
            "last_update": self.last_update,
        }


class JobService:
    def __init__(
        self,
        socketio,
        runner_factory=InProcessScraperRunner,
        rate_limit: int = 2,
        rate_limit_window: int = 60,
        cleanup_after_seconds: int = 900,
        metrics_window_seconds: int = 180,
        max_concurrent_jobs: int = 2,
    ) -> None:
        self._socketio = socketio
        self._runner_factory = runner_factory
        self._lock = threading.Lock()
        self._jobs: Dict[str, ScrapingJobState] = {}
        self._job_starts: Deque[float] = deque()
        self._rate_limit = rate_limit
        self._rate_limit_window = rate_limit_window
        self._cleanup_after_seconds = cleanup_after_seconds
        self._metrics_window_seconds = metrics_window_seconds
        self._max_concurrent_jobs = max_concurrent_jobs

    # Public API -----------------------------------------------------------------
    def start_job(
        self,
        job_id: str,
        search_query: str,
        total_results: int,
        output_file: str,
        append_mode: bool,
        fast_append: bool,
    ) -> ScrapingJobState:
        with self._lock:
            self._cleanup_finished_jobs_locked()
            self._enforce_rate_limit_locked()
            if self._current_running_jobs_locked() >= self._max_concurrent_jobs:
                raise RateLimitExceeded("Maximum concurrent scraping jobs in progress")

            state = ScrapingJobState(
                job_id=job_id,
                search_query=search_query,
                total_results=total_results,
                output_file=output_file,
                append_mode=append_mode,
                fast_append=fast_append,
                status="running",
                last_update=time.time(),
            )
            self._jobs[job_id] = state
            self._job_starts.append(time.time())

        config = ScraperJobConfig(
            search_query=search_query,
            total_results=total_results,
            output_file=output_file,
            append_mode=append_mode,
            ultra_fast_append=fast_append,
        )

        thread = threading.Thread(target=self._run_job, args=(state, config), daemon=True)
        thread.start()
        return state

    def get_job(self, job_id: str) -> ScrapingJobState:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise JobNotFound(job_id)
            return job

    def list_jobs(self) -> List[ScrapingJobState]:
        with self._lock:
            return list(self._jobs.values())

    def get_preview(self, job_id: str, limit: int = 10) -> Dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise JobNotFound(job_id)
            records = list(job.recent_records)[-limit:]
            columns = list(records[0].keys()) if records else []
            return {
                "records": records,
                "columns": columns,
                "result_count": job.result_count,
                "progress": job.progress,
                "total_results": job.total_results,
                "status": job.status,
            }

    # Internal orchestration -----------------------------------------------------
    def _run_job(self, state: ScrapingJobState, config: ScraperJobConfig) -> None:
        runner = self._runner_factory()
        callbacks = ScraperCallbacks(
            progress=lambda payload: self._handle_progress(state.job_id, payload),
            metrics=lambda payload: self._handle_metrics(state.job_id, payload),
            record_batch=lambda payload: self._handle_record_batch(state.job_id, payload),
        )

        result = runner.run(config, callbacks)

        with self._lock:
            finished = datetime.utcnow()
            state.end_time = finished
            state.elapsed_seconds = (finished - state.start_time).total_seconds()
            if state.status not in {"interrupted", "failed"}:
                if result.success:
                    state.status = "completed"
                else:
                    state.status = "failed"
            if not result.success and result.error:
                state.error_message = str(result.error)
            if state.last_update is None:
                state.last_update = time.time()
            self._update_metrics_locked(state)
            progress_event = self._build_progress_payload_locked(state)
            metrics_event = self._build_metrics_payload_locked(state)

        self._socketio.emit("job_progress", progress_event)
        self._socketio.emit("job_metrics", metrics_event)
        self._cleanup_finished_jobs()

    def _handle_progress(self, job_id: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job.progress = payload.get("progress", job.progress)
            job.result_count = payload.get("current", job.result_count)
            error_message = payload.get("error_message")
            if error_message:
                job.error_message = error_message
            status = payload.get("status")
            if status:
                job.status = status
            elif job.status == "pending":
                job.status = "running"
            job.last_update = payload.get("timestamp", time.time())
            job.progress_samples.append((job.last_update, job.result_count))
            self._prune_progress_samples_locked(job)
            progress_event = self._build_progress_payload_locked(job)

        self._socketio.emit("job_progress", progress_event)

    def _handle_metrics(self, job_id: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            elapsed = payload.get("elapsed_seconds")
            if elapsed is not None:
                job.elapsed_seconds = float(elapsed)
            else:
                job.elapsed_seconds = time.time() - job.start_time.timestamp()
            self._update_metrics_locked(job)
            metrics_event = self._build_metrics_payload_locked(job)

        self._socketio.emit("job_metrics", metrics_event)

    def _handle_record_batch(self, job_id: str, payload: Dict[str, Any]) -> None:
        records = payload.get("records")
        if not records:
            return
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            for record in records:
                job.recent_records.append(record)
            total_scraped = payload.get("total_scraped")
            if total_scraped is not None:
                try:
                    job.result_count = int(total_scraped)
                except (TypeError, ValueError):
                    pass
            preview = list(job.recent_records)
            record_event = {
                "job_id": job_id,
                "records": records,
                "preview": preview,
                "result_count": job.result_count,
            }

        self._socketio.emit("job_record_batch", record_event)

    # Helpers -------------------------------------------------------------------
    def _enforce_rate_limit_locked(self) -> None:
        now_ts = time.time()
        while self._job_starts and now_ts - self._job_starts[0] > self._rate_limit_window:
            self._job_starts.popleft()
        if len(self._job_starts) >= self._rate_limit:
            raise RateLimitExceeded("Too many scraping jobs started recently")

    def _current_running_jobs_locked(self) -> int:
        return sum(1 for job in self._jobs.values() if job.status == "running")

    def _cleanup_finished_jobs(self) -> None:
        with self._lock:
            self._cleanup_finished_jobs_locked()

    def _cleanup_finished_jobs_locked(self) -> None:
        cutoff = time.time() - self._cleanup_after_seconds
        to_delete = [
            job_id
            for job_id, job in self._jobs.items()
            if job.end_time and job.end_time.timestamp() < cutoff
        ]
        for job_id in to_delete:
            del self._jobs[job_id]

    def _prune_progress_samples_locked(self, job: ScrapingJobState) -> None:
        if not job.progress_samples:
            return
        latest_ts = job.progress_samples[-1][0]
        cutoff = latest_ts - self._metrics_window_seconds
        while job.progress_samples and job.progress_samples[0][0] < cutoff:
            job.progress_samples.popleft()

    def _update_metrics_locked(self, job: ScrapingJobState) -> None:
        if len(job.progress_samples) < 2:
            job.throughput_per_minute = 0.0
            job.eta_seconds = None
            return
        first_ts, first_count = job.progress_samples[0]
        last_ts, last_count = job.progress_samples[-1]
        delta_count = last_count - first_count
        delta_time = last_ts - first_ts
        if delta_time <= 0 or delta_count <= 0:
            job.throughput_per_minute = 0.0
            job.eta_seconds = None
            return
        throughput_per_second = delta_count / delta_time
        job.throughput_per_minute = throughput_per_second * 60
        remaining = max(job.total_results - job.result_count, 0)
        job.eta_seconds = remaining / throughput_per_second if throughput_per_second > 0 else None

    def _build_progress_payload_locked(self, job: ScrapingJobState) -> Dict[str, Any]:
        end_time = job.end_time.strftime('%Y-%m-%d %H:%M:%S') if job.end_time else None
        return {
            "job_id": job.job_id,
            "progress": job.progress,
            "result_count": job.result_count,
            "status": job.status,
            "timestamp": job.last_update or time.time(),
            "error_message": job.error_message,
            "end_time": end_time,
        }

    def _build_metrics_payload_locked(self, job: ScrapingJobState) -> Dict[str, Any]:
        return {
            "job_id": job.job_id,
            "throughput_per_minute": job.throughput_per_minute,
            "eta_seconds": job.eta_seconds,
            "elapsed_seconds": job.elapsed_seconds,
            "result_count": job.result_count,
            "total_results": job.total_results,
        }
