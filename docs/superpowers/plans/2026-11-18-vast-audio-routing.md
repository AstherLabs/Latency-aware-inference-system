# Scheduled Vast audio integration

Goal: Switch the staging product backend to App -> backend -> scheduler -> private streaming gRPC Vast worker -> existing audio HTTP/model services for vital_v7_3, vital_v7_5, vital_v7_5_chord. Preserve App API, source/model/artifact identities, authentication/database environments, model code and weights. Chord is the existing UI name of the third variant.

Architecture: Keep the backend's asynchronous HTTP job contract, served at gateway /audio. Persist gateway audio request state/input in its own private volume, submit small path/identity tasks to the existing scheduler/worker pool, and send file bytes through client-streaming gRPC. An Execute task spans actual inference completion, so the scheduler owns GPU work, not merely HTTP submission. Artifact downloads use server-streaming gRPC. Use the existing request-ID-aware Vast HTTP route, adding the same tested route to V7.3. Idempotent replays require identical model and audio hash; never fall back to another model or legacy POST.

Wire protocol (new service, existing protobuf WorkerService remains unchanged):
Service synthx.audio.v1.AudioWorker
- Execute: stream bytes -> JSON bytes. First frame is finite JSON metadata {version:1,model,request_id,input_sha256,input_size,checkpoint_sha256}; subsequent frames raw audio, <=64 KiB each; max input 200 MiB. Returns {job_id,model,state:'completed',result:<original Vast result dict>} or {error:{code,message,retryable}}.
- Artifact: JSON {model,request_id,kind:'vital'|'wav'} -> stream bytes. Check result identity and artifact hash.
- Health: JSON {} -> JSON {ready,models,checkpoints,...}. No generation.
- Fixed HTTP origins on Vast only: v7.3 http://127.0.0.1:18081; v7.5/chord http://127.0.0.1:18085.
- IDs lowercase hex32. v7.3 checkpoint 3059361d046c2c04fd40ae3d29c97e1c7d56e167f93b4b945d96aee1d4e196de. v7.5/chord dd9efb895f2903941ead3e5e8761f0c37f07a522e742ef04819d75e059586aa3.

Shared module system/audio_protocol.py interfaces:
- CHECKPOINTS mapping; KIND='synthx_audio'; CHUNK_BYTES=65536; MAX_INPUT_BYTES=209715200.
- AudioWorkerError(code,message,retryable=False).
- execute_remote(channel,payload,timeout): payload contains kind,model,request_id,input_path,input_sha256,input_size,checkpoint_sha256. Return Execute dictionary or raise AudioWorkerError.
- remote_artifact(channel,model,request_id,kind,timeout=60): iterable bytes.
- probe_remote(channel,timeout=5): health dict.
Use grpc generic method serializers, versioned service names, and bounded finite JSON; no protobuf regeneration required.

Gateway manager interface system/audio_jobs.py:
- AudioJobManager(root, queue, worker_address, deployment_gate=None,max_pending=4,job_timeout=1700).
- start()/stop(); async submit(model,request_id,async_chunks) -> status; status(request_id), result(request_id); artifact(request_id,kind) -> byte iterable; health() -> dict.
- create_router(manager_provider,authorize_dependency) in system/audio_api.py, registered prefix /audio.
Routes POST /requests/{id}/{model}, POST /jobs/{model} (generate stable id once), GET /jobs/{id}, /result, /artifacts/{kind}, GET /health. All require backend service authorization.
- Atomic persistent state and process ownership lock; bounded admission/disk; restart requeues nonterminal jobs with same request IDs; no duplicate compute on replay.
- Tasks use KIND and above payload. Background monitor handles Task terminal results. Transport/busy retry bounded by original absolute deadline, new attempt task references same immutable job.
- Persist original validated Vast result; final status only completed after model/checkpoint/input identities match. Input files may be removed after durable completion while retaining fingerprint for duplicates.
- Deployment gate holds admission across each accepted nonterminal job; reject new work while draining; recoverable GETs remain available.
- stop preserves pending records, cancels no remote computation, and never marks recoverable work completed.

Root integration:
- grpc_worker_pool: allow KIND, route through worker capability, run execute_remote on existing channel instead of serializing local file path through ProcessBatch. Mark transport errors terminal per attempt; manager owns idempotent recovery. Existing text/legacy behavior unchanged.
- api_server lifespan creates/stops optional manager only when SYNTHX_AUDIO_ENABLED=1; register router. Extend protected deployment active accounting via manager gate. Expose model-aware audio health separately.
- static worker 73 gains synthx_audio capability, uses current model-tunnel:50061; no old checkpoint substitution.
- backend client uses explicit SYNTHFLOW_AUDIO_TRANSPORT=grpc_scheduler, SYNTHFLOW_AUDIO_SCHEDULER_URL=http://gateway:8000/audio and SYNTHFLOW_AUDIO_GATEWAY_TOKEN. Requires token; no direct fallback; no redirect leakage. Existing direct mode stays for rollback only.
- No database/schema/auth changes.

Tasks:
1. TDD protocol/worker with real local gRPC+HTTP boundaries, malformed identity/hash/model/size, idempotent replay, cross-model isolation, cancellation/restart and serialized inference.
2. TDD durable gateway manager/router for 3 models, mismatch replay, bounded queue, auth/drain, transport retry and restart, real streamed artifacts.
3. TDD backend routing/auth/no-fallback and common Vast idempotent request route.
4. Root wire integrations and full test suites; fresh review.
5. Snapshot/provision isolated worker deps on Vast, Supervisor on loopback50061. Do not touch training or management services. V7.3 HTTP restart only after idle check and backup.
6. Build/pin new platform/backend images, honor shared deployment lock and existing draining/rollback; persist reviewed source/receipts/configuration, not ephemeral docker exec edits.
7. Real three-model test via scheduler, validate native/WAV SHA and checkpoint, then switch staging backend and test its actual client through complete path. Keep dev rollout separate until staging accepted; never silently change its auth/database identity.

Review focus: 200MiB input streaming; ambiguous disconnect after submission; model choice immutable on retry; durable job state on gateway/worker restart; deployment drain counts background inference; worker readiness proves correct models; result artifact SHA; no public unauthenticated ports; no bypass of scheduler in enabled backend.

