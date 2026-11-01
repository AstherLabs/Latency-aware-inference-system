"""PostgreSQL lease and completion fencing for SynthX inference jobs."""
import uuid
import math


class JobStore:
    def __init__(self, database_url, *, statement_timeout_seconds=5):
        if not database_url:
            raise ValueError("DATABASE_URL is required for durable jobs")
        if not math.isfinite(statement_timeout_seconds) or statement_timeout_seconds <= 0:
            raise ValueError("statement timeout must be positive and finite")
        self._statement_timeout_ms = max(1, int(statement_timeout_seconds * 1000))
        # Optional dependency: simulator deployments need no database driver.
        import psycopg
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
        self._connect = psycopg.connect
        self._row_factory = dict_row
        self._json = Jsonb
        self._database_url = database_url

    def _connection(self):
        return self._connect(
            self._database_url, row_factory=self._row_factory, connect_timeout=5,
            options=f"-c statement_timeout={self._statement_timeout_ms} "
                    f"-c lock_timeout={min(1000, self._statement_timeout_ms)}",
        )

    @staticmethod
    def _normalize(row):
        if row is None:
            return None
        return {key: str(value) if isinstance(value, uuid.UUID) else value for key, value in row.items()}

    def get(self, job_id):
        with self._connection() as conn:
            return self._normalize(conn.execute("SELECT * FROM synthx.inference_jobs WHERE job_id=%s", (job_id,)).fetchone())

    def claim(self, *, job_id=None, lease_seconds=120):
        """Atomically claim one due row; a live lease is never eligible."""
        attempt = uuid.uuid4()
        with self._connection() as conn:
            row = conn.execute("""
                WITH candidate AS (
                    SELECT job_id FROM synthx.inference_jobs
                    WHERE (%s::uuid IS NULL OR job_id=%s::uuid)
                      AND deadline_at > now()
                      AND ((status='queued' AND next_attempt_at <= now())
                        OR (status='running' AND lease_until <= now()))
                    ORDER BY next_attempt_at, created_at, job_id
                    FOR UPDATE SKIP LOCKED LIMIT 1
                )
                UPDATE synthx.inference_jobs AS j SET
                    status='running', attempt_id=%s, attempts=j.attempts+1,
                    lease_until=LEAST(now() + %s * interval '1 second', j.deadline_at),
                    updated_at=now()
                FROM candidate WHERE j.job_id=candidate.job_id RETURNING j.*
                """, (job_id, job_id, attempt, lease_seconds)).fetchone()
            return self._normalize(row)

    def renew(self, job_id, attempt_id, lease_seconds):
        with self._connection() as conn:
            return conn.execute("""UPDATE synthx.inference_jobs SET
                lease_until=LEAST(now() + %s * interval '1 second', deadline_at), updated_at=now()
                WHERE job_id=%s AND attempt_id=%s AND status='running'
                  AND lease_until > now() AND deadline_at > now()""",
                (lease_seconds, job_id, attempt_id)).rowcount == 1

    def complete(self, job_id, attempt_id, *, result=None, error=None):
        with self._connection() as conn:
            return conn.execute("""UPDATE synthx.inference_jobs SET status=%s,
                result=%s, error=%s, lease_until=NULL, updated_at=now()
                WHERE job_id=%s AND attempt_id=%s AND status='running'
                  AND lease_until > now() AND deadline_at > now()""",
                ("succeeded" if error is None else "failed", self._json(result) if result is not None else None,
                 self._json(error) if error is not None else None, job_id, attempt_id)).rowcount == 1

    def defer(self, job_id, attempt_id, error, retry_seconds=5):
        with self._connection() as conn:
            return conn.execute("""UPDATE synthx.inference_jobs SET status='queued',
                lease_until=NULL, next_attempt_at=now() + %s * interval '1 second',
                error=%s, updated_at=now()
                WHERE job_id=%s AND attempt_id=%s AND status='running'
                  AND lease_until > now() AND deadline_at > now()""",
                (retry_seconds, self._json(error), job_id, attempt_id)).rowcount == 1

    def expire_deadlines(self):
        with self._connection() as conn:
            return conn.execute("""UPDATE synthx.inference_jobs SET status='failed',
                lease_until=NULL, error=%s, updated_at=now()
                WHERE status IN ('queued','running') AND deadline_at <= now()""",
                (self._json({"code": "deadline_exceeded", "message": "job deadline exceeded"}),)).rowcount
