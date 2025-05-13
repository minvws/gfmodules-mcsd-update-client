from datetime import datetime
import time
from unittest.mock import MagicMock

from app.services.scheduler import Scheduler

def test_scheduler_start_and_stop() -> None:
    mock_function = MagicMock()
    scheduler = Scheduler(function=mock_function, delay=1, max_logs_entries=5)

    scheduler.start()
    time.sleep(2)  # Allow the scheduler to run the function at least once
    scheduler.stop()

    # Verify the function was called at least once
    assert mock_function.call_count > 0


def test_scheduler_update_runner() -> None:
    mock_function = MagicMock()
    scheduler = Scheduler(function=mock_function, delay=1, max_logs_entries=2)

    # Simulate runner updates
    scheduler.update_runner(start_time=1.0, end_time=2.0)
    scheduler.update_runner(start_time=3.0, end_time=4.0)
    scheduler.update_runner(start_time=5.0, end_time=6.0)

    # Verify the runner logs are capped at max_logs_entries
    runner_history = scheduler.get_runner_history()
    assert len(runner_history) == 2
    assert runner_history[0]["runner_id"] == 2
    assert runner_history[1]["runner_id"] == 3


def test_scheduler_get_runner_history() -> None:
    mock_function = MagicMock()
    scheduler = Scheduler(function=mock_function, delay=1, max_logs_entries=5)

    # Simulate runner updates
    scheduler.update_runner(start_time=1.0, end_time=2.0)
    scheduler.update_runner(start_time=3.0, end_time=4.0)

    # Verify the runner history
    runner_history = scheduler.get_runner_history()
    assert len(runner_history) == 2
    assert runner_history[0]["started_at"] == datetime.fromtimestamp(1.0).isoformat()
    assert runner_history[0]["finished_at"] == datetime.fromtimestamp(2.0).isoformat()
    assert runner_history[1]["started_at"] == datetime.fromtimestamp(3.0).isoformat()
    assert runner_history[1]["finished_at"] == datetime.fromtimestamp(4.0).isoformat()