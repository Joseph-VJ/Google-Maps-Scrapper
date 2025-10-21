import time

import pytest

from job_service import JobService, JobNotFound, RateLimitExceeded, ScrapingJobState
from scraper_runner import ScraperRunResult


class SocketStub:
    def __init__(self) -> None:
        self.emitted = []

    def emit(self, event: str, payload):  # pragma: no cover - simple stub
        self.emitted.append((event, payload))


class ImmediateRunner:
    def __init__(self, sequence=None) -> None:
        self.sequence = sequence or []

    def run(self, config, callbacks) -> ScraperRunResult:
        for kind, payload in self.sequence:
            if kind == 'progress' and callbacks.progress:
                callbacks.progress(payload)
            elif kind == 'metrics' and callbacks.metrics:
                callbacks.metrics(payload)
            elif kind == 'records' and callbacks.record_batch:
                callbacks.record_batch(payload)
        return ScraperRunResult(success=True, elapsed_seconds=0.1)


def create_service(socket_stub, runner_sequence=None, **kwargs) -> JobService:
    runner_sequence = runner_sequence or []

    def factory():
        return ImmediateRunner(runner_sequence)

    return JobService(
        socket_stub,
        runner_factory=factory,
        rate_limit=kwargs.get('rate_limit', 5),
        rate_limit_window=kwargs.get('rate_limit_window', 60),
        cleanup_after_seconds=kwargs.get('cleanup_after_seconds', 3600),
        metrics_window_seconds=kwargs.get('metrics_window_seconds', 120),
        max_concurrent_jobs=kwargs.get('max_concurrent_jobs', 2),
    )


def test_job_service_updates_progress_and_metrics():
    socket_stub = SocketStub()
    service = create_service(socket_stub)
    job_id = 'job-progress'
    state = ScrapingJobState(
        job_id=job_id,
        search_query='coffee shops',
        total_results=100,
        output_file='results.csv',
        append_mode=False,
        fast_append=False,
    )
    service._jobs[job_id] = state

    service._handle_progress(job_id, {
        'job_id': job_id,
        'progress': 10.0,
        'current': 10,
        'timestamp': 1_000.0,
    })
    service._handle_progress(job_id, {
        'job_id': job_id,
        'progress': 25.0,
        'current': 25,
        'timestamp': 1_010.0,
    })
    service._handle_metrics(job_id, {
        'elapsed_seconds': 10.0,
        'timestamp': 1_010.0,
    })

    job_state = service.get_job(job_id)
    assert job_state.status == 'running'
    assert job_state.progress == 25.0
    assert job_state.result_count == 25
    assert job_state.throughput_per_minute >= 0.0
    assert job_state.elapsed_seconds == 10.0

    progress_events = [event for event, _ in socket_stub.emitted if event == 'job_progress']
    metrics_events = [payload for event, payload in socket_stub.emitted if event == 'job_metrics']
    assert len(progress_events) == 2
    assert metrics_events
    last_metrics = metrics_events[-1]
    assert last_metrics['job_id'] == job_id
    assert last_metrics['elapsed_seconds'] == 10.0


def test_job_service_record_batch_preview():
    socket_stub = SocketStub()
    service = create_service(socket_stub)
    job_id = 'job-records'
    state = ScrapingJobState(
        job_id=job_id,
        search_query='plumbers',
        total_results=50,
        output_file='records.csv',
        append_mode=True,
        fast_append=False,
    )
    service._jobs[job_id] = state

    payload = {
        'records': [
            {'name': 'Alpha Plumbing', 'phone': '123'},
            {'name': 'Beta Plumbing', 'phone': '456'},
        ],
        'total_scraped': 2,
    }
    service._handle_record_batch(job_id, payload)

    preview = service.get_preview(job_id)
    assert preview['records']
    assert preview['records'][0]['name'] == 'Alpha Plumbing'
    assert preview['result_count'] == 2

    record_events = [item for item in socket_stub.emitted if item[0] == 'job_record_batch']
    assert record_events
    assert record_events[-1][1]['records'][0]['name'] == 'Alpha Plumbing'


def test_job_service_rate_limit_enforced():
    socket_stub = SocketStub()
    service = create_service(
        socket_stub,
        runner_sequence=[('progress', {'job_id': 'job-rate', 'progress': 100.0, 'current': 5, 'timestamp': time.time()})],
        rate_limit=1,
        rate_limit_window=60,
        max_concurrent_jobs=2,
    )

    service.start_job('job-rate-1', 'coffee', 5, 'file.csv', False, False)
    time.sleep(0.1)

    with pytest.raises(RateLimitExceeded):
        service.start_job('job-rate-2', 'coffee', 5, 'file2.csv', False, False)

    with pytest.raises(JobNotFound):
        service.get_job('non-existent-job')
