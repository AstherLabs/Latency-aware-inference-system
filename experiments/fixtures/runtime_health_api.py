"""Acceptance-only API process with a file-controlled dispatch stall.

This fixture is never imported by the production API. No fault HTTP endpoint is
added to the service. A fresh process removes the injected handler stall.
"""
import os
import json
from pathlib import Path
import threading

from system import api_server
from system.scheduler import Scheduler
from system import job_store


class TraceJobStore(job_store.JobStore):
    """Record database-issued successful claim times for lease assertions."""
    def claim(self, **kwargs):
        row = super().claim(**kwargs)
        if row is not None:
            record = {"job_id": row["job_id"], "attempt_id": row["attempt_id"],
                      "claimed_at": row["updated_at"].isoformat()}
            with open(os.environ["ACCEPTANCE_CLAIM_LOG"], "a") as log:
                log.write(json.dumps(record) + "\n")
        return row


class FaultScheduler(Scheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        dispatch = self._handler
        gate = Path(os.environ['ACCEPTANCE_STALL_FILE'])
        entered = Path(os.environ['ACCEPTANCE_STALL_ENTERED_FILE'])

        def handler(batch):
            if batch and gate.exists():
                entered.write_text('dispatch stalled\n')
                threading.Event().wait()
            return dispatch(batch)
        self._handler = handler


if __name__ == '__main__':
    import uvicorn
    api_server.Scheduler = FaultScheduler
    job_store.JobStore = TraceJobStore
    uvicorn.run(api_server.app, host='0.0.0.0', port=8000, log_level='warning')
