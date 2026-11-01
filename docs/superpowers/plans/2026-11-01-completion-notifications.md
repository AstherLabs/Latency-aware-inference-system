# Completion notifications and bounded synchronous requests

**Goal:** Reduce API polling overhead while bounding synchronous waiters and cleaning abandoned ephemeral tasks. SQL jobs retain ownership and recovery behavior.

**Architecture:** Task completion uses loop-owned futures registered atomically under the existing task lock; worker threads notify with call_soon_threadsafe outside that lock. A shared ASGI admission counter covers /predict and /text/tasks before body parsing. Endpoint wait races completion/deadline with an ASGI disconnect listener and always releases its waiter. Cancellation purges queue/backlog/proxy waiting entries and never closes a shared active RPC.

**Tasks**
- [x] Write and observe failing completion, timeout, cancellation, admission, and purge tests.
- [x] Implement Task wait_finished, completion publication, endpoint waiting, admission middleware, and queue cleanup; run regressions.
- [x] Build two isolated images from hashed working-tree snapshots, not historical HEAD. Repeat identical open-loop normal/overload/recovery load in A/B and B/A orders, retaining generator validity and refusal counts.
- [x] Measure API CPU seconds, RSS, P95/P99, throughput, errors and pending queues. Separately verify configured admission limits and disconnect cleanup. Produce standalone plots and a candid report, including regressions and limitations.
- [x] Verify final repository files and publish only authorized changes. No production deployment or git push.

**Decisions:** Default MAX_SYNC_REQUESTS=1024 per API process, shared by both synchronous routes. Durable /jobs is excluded. No load-aware routing or retry-policy changes in this experiment. Compare both variants under the same sub-limit offered load to avoid confusing rejection with efficiency. The admission stress check is reported independently.
