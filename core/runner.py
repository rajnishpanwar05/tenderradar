# =============================================================================
# pipeline/runner.py — Concurrent Job Runner
#
# Runs all scraper jobs with:
#   • Thread-based parallelism  (I/O-bound scrapers don't need multiprocessing)
#   • Per-job timeout           (kills stuck scrapers without blocking the run)
#   • Automatic retry           (with exponential back-off)
#   • Selenium concurrency cap  (max 2 Chrome instances at once via semaphore)
#   • CAPTCHA jobs sequential   (always run one at a time, no timeout skip)
#
# Parallelism model:
#   Non-captcha jobs → ThreadPoolExecutor(MAX_WORKERS=7)
#     └── Selenium jobs within the pool use _SELENIUM_SEM(2)
#         so at most 2 headless Chrome instances run simultaneously
#   Captcha jobs → sequential, after all parallel jobs complete
#
# Expected speedup vs. sequential main.py:
#   25 scrapers × avg 3 min = ~75 min sequential
#   With 7 parallel workers → ~15–20 min total run time
# =============================================================================

import importlib
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future, wait as _cf_wait, FIRST_COMPLETED
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from core.registry import ScraperJob
from monitoring.scraper_health_manager import should_skip, should_smart_retry

logger = logging.getLogger("tenderradar.runner")

# Hard cap: no more than 2 Selenium (headless Chrome) instances at once.
# Selenium is ~300–500 MB RAM per instance; 2 is safe on most machines.
_SELENIUM_SEM = threading.Semaphore(2)


# ── Result container ─────────────────────────────────────────────────────────

@dataclass
class JobResult:
    flag:         str
    label:        str
    new_tenders:  List[dict]   = field(default_factory=list)
    all_rows:     List[dict]   = field(default_factory=list)
    elapsed:      float        = 0.0
    status:       str          = "skip"   # ok | warn | fail | timeout | skip
    error:        str          = ""
    attempts:     int          = 0
    started_at:   str          = ""
    finished_at:  str          = ""


# ── Runner ───────────────────────────────────────────────────────────────────

class JobRunner:
    """
    Execute a list of ScraperJobs reliably.

    Usage:
        runner  = JobRunner()
        results = runner.run(jobs)
        # results: List[JobResult] — one per job, in registry order
    """

    MAX_WORKERS:  int   = min(10, (os.cpu_count() or 2) * 2)  # ISSUE 10: dynamic, cap 10
    RETRY_DELAY:  float = 30.0   # base delay between retries (doubles per attempt)
    ZERO_RETRY:   bool  = True   # retry once if a scraper returns 0 rows

    def run(self, jobs: List[ScraperJob]) -> List[JobResult]:
        """
        Run all jobs. Non-captcha jobs run in parallel; captcha jobs run last
        in sequential order (they need manual interaction).

        Returns results in the same order as `jobs`.
        """
        if not jobs:
            return []

        parallel_jobs = [j for j in jobs if not j.needs_captcha]
        captcha_jobs  = [j for j in jobs if j.needs_captcha]

        # Maintain a flag→result map so we can reconstruct original order
        results: Dict[str, JobResult] = {}

        # ── Phase 1: parallel ──────────────────────────────────────────────
        if parallel_jobs:
            logger.info(
                f"[runner] Starting {len(parallel_jobs)} parallel jobs "
                f"(max_workers={self.MAX_WORKERS})"
            )
            with ThreadPoolExecutor(
                max_workers = self.MAX_WORKERS,
                thread_name_prefix = "trscraper",
            ) as pool:
                future_to_job: Dict[Future, ScraperJob] = {
                    pool.submit(self._run_single, job): job
                    for job in parallel_jobs
                }
                # Track per-job deadlines so slow jobs are timed out individually.
                # as_completed() alone can't enforce per-job timeouts because it only
                # yields already-complete futures. Instead we poll with wait() using a
                # short interval and check each pending job against its deadline.
                deadline: Dict[Future, float] = {
                    f: time.time() + job.timeout + 60
                    for f, job in future_to_job.items()
                }
                pending = set(future_to_job.keys())
                while pending:
                    done, pending = _cf_wait(pending, timeout=5, return_when=FIRST_COMPLETED)
                    for future in done:
                        job = future_to_job[future]
                        try:
                            result = future.result()
                        except Exception as exc:
                            result = JobResult(
                                flag=job.flag, label=job.label,
                                status="fail", error=str(exc), attempts=1,
                            )
                            logger.error(f"[runner] {job.label}: unexpected runner error: {exc}")
                        results[job.flag] = result
                    # Check remaining pending futures for timeout
                    now = time.time()
                    timed_out = {f for f in pending if now >= deadline[f]}
                    for future in timed_out:
                        job = future_to_job[future]
                        results[job.flag] = JobResult(
                            flag=job.flag, label=job.label,
                            status="timeout",
                            error=f"Exceeded {job.timeout + 60}s deadline",
                            attempts=1,
                        )
                        logger.error(
                            f"[runner] {job.label}: timed out after {job.timeout + 60}s "
                            f"— job thread will finish in background but result is discarded"
                        )
                        pending.discard(future)

        # ── Phase 2: captcha jobs (sequential, user interaction needed) ───
        for job in captcha_jobs:
            logger.info(f"[runner] Starting CAPTCHA job: {job.label}")
            results[job.flag] = self._run_single(job)

        # Return in original job order
        return [results[j.flag] for j in jobs if j.flag in results]

    # ── Per-job execution ─────────────────────────────────────────────────────

    def _run_single(self, job: ScraperJob) -> JobResult:
        """
        Run one job with retry + optional Selenium semaphore.
        Called in a worker thread for parallel jobs.
        """
        if should_skip(job.label):
            logger.warning(
                f"[runner] {job.label}: skipped because health manager marked it unstable"
            )
            now = datetime.now().isoformat(timespec="seconds")
            return JobResult(
                flag=job.flag,
                label=job.label,
                status="skip",
                error="marked unstable by health manager",
                attempts=0,
                started_at=now,
                finished_at=now,
            )

        # Selenium jobs must wait for a slot
        sem_acquired = False
        if job.group == "selenium":
            logger.debug(f"[runner] {job.label}: waiting for Selenium slot…")
            _SELENIUM_SEM.acquire()
            sem_acquired = True
            logger.debug(f"[runner] {job.label}: Selenium slot acquired")

        try:
            return self._execute_with_retry(job)
        finally:
            if sem_acquired:
                _SELENIUM_SEM.release()
                logger.debug(f"[runner] {job.label}: Selenium slot released")

    def _execute_with_retry(self, job: ScraperJob) -> JobResult:
        """
        Call job.module.run() up to max_retries times.

        Retry conditions:
          1. Unhandled exception → always retry (up to max_retries)
          2. Zero rows returned, no exception → retry once if ZERO_RETRY=True
             (protects against transient empty pages, not selector drift)

        Back-off: RETRY_DELAY * attempt (30s, 60s, …)
        """
        max_attempts = max(1, job.max_retries + 1)
        last_error   = ""

        for attempt in range(1, max_attempts + 1):
            start = time.time()
            started_at = datetime.now().isoformat(timespec="seconds")

            try:
                mod = importlib.import_module(job.module)
                new_tenders, all_rows = mod.run()
                elapsed     = time.time() - start
                finished_at = datetime.now().isoformat(timespec="seconds")

                if all_rows:
                    logger.info(
                        f"[runner] {job.label}: OK — "
                        f"{len(all_rows)} rows, {len(new_tenders)} new "
                        f"[attempt {attempt}, {elapsed:.0f}s]"
                    )
                    return JobResult(
                        flag=job.flag, label=job.label,
                        new_tenders=new_tenders, all_rows=all_rows,
                        elapsed=elapsed, status="ok",
                        attempts=attempt,
                        started_at=started_at, finished_at=finished_at,
                    )

                # Zero rows — might be transient
                logger.warning(
                    f"[runner] {job.label}: 0 rows on attempt {attempt}"
                    f"/{max_attempts} [{elapsed:.0f}s]"
                )
                if self.ZERO_RETRY and attempt < max_attempts:
                    if not should_smart_retry(job.label, current_rows=len(all_rows or [])):
                        logger.info(
                            f"[runner] {job.label}: skipping zero-row retry based on portal history"
                        )
                        return JobResult(
                            flag=job.flag, label=job.label,
                            new_tenders=[], all_rows=[],
                            elapsed=elapsed, status="warn",
                            error="zero rows (retry skipped by health history)",
                            attempts=attempt,
                            started_at=started_at, finished_at=finished_at,
                        )
                    wait = self.RETRY_DELAY * attempt
                    logger.info(
                        f"[runner] {job.label}: retrying zero-row result in {wait:.0f}s…"
                    )
                    time.sleep(wait)
                    continue

                return JobResult(
                    flag=job.flag, label=job.label,
                    new_tenders=[], all_rows=[],
                    elapsed=elapsed, status="warn",
                    error="zero rows",
                    attempts=attempt,
                    started_at=started_at, finished_at=finished_at,
                )

            except Exception as exc:
                elapsed    = time.time() - start
                last_error = str(exc)
                logger.error(
                    f"[runner] {job.label}: EXCEPTION on attempt "
                    f"{attempt}/{max_attempts} [{elapsed:.0f}s]: {exc}"
                )
                if attempt < max_attempts:
                    wait = self.RETRY_DELAY * attempt
                    logger.info(
                        f"[runner] {job.label}: retrying in {wait:.0f}s…"
                    )
                    time.sleep(wait)

        # All attempts exhausted
        finished_at = datetime.now().isoformat(timespec="seconds")
        return JobResult(
            flag=job.flag, label=job.label,
            new_tenders=[], all_rows=[],
            elapsed=0.0, status="fail",
            error=last_error,
            attempts=max_attempts,
            started_at=started_at, finished_at=finished_at,
        )
