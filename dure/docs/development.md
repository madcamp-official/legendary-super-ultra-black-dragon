# 개발과 릴리스 절차

## 개발 환경

```bash
python3 -m pip install -e '.[test]'
python3 -m compileall -q src tests
python3 -m unittest discover -v
git diff --check
```

단위 테스트는 SQLite, 가짜 호스트 명령, FastAPI 테스트 클라이언트를 사용합니다. GPU, Docker 데몬, PostgreSQL, 인터넷 없이 실행 가능해야 합니다.

기본 패키지는 Ubuntu 22.04용 노드 패키지를 가볍게 유지하기 위해 외부 Python 의존성을 두지 않습니다. 중앙 제어면 의존성은 `server` 추가 의존성, 전체 테스트 의존성은 `test` 추가 의존성에 있습니다.

host bootstrap 테스트는 임시 root와 `FakeRunner`만 사용하며 실제 GPU, Docker daemon, systemd, APT 저장소나 인터넷을 요구하지 않습니다. 기본 preview의 무변경성, NVIDIA driver 명령 부재, exact 공식 URL·primary key·패키지·version pin, 전송 중 signing key 출력 상한, APT 제거 금지, Agent/join lock, 충돌·부분 설치·unsafe 또는 APT가 통과할 수 없는 경로 거부, 재시작 직전 workload 재검사, 재시작 후 exact runtime 검사와 실패 뒤 `daemon.json` 복구를 정상·거부 경로로 함께 검증합니다. bootstrap을 중앙 task나 사용자 제공 셸·Docker 옵션 실행 경로와 연결하면 안 됩니다.

로컬 controller 예시:

```bash
export DURE_DATABASE_URL=sqlite:////tmp/dure-control.db
export DURE_ADMIN_TOKEN=development-only
dure-server --migrate
dure-server --host 127.0.0.1 --port 8081
```

## Git 훅

저장소는 `.githooks/`에 native hook을 추적합니다.

```bash
cd /path/to/legendary-super-ultra-black-dragon
git config --local core.hooksPath .githooks
git config --show-origin --get core.hooksPath
git hook run pre-commit
git hook run pre-push -- origin "$(git remote get-url origin)"
```

- hook은 저장소 루트에서 실행되지만 Dure source는 `dure/` 아래에 있으므로, tracked hook은 그 경로를 명시적으로 해석합니다. Python은 `DURE_PYTHON`, 활성 virtualenv, 저장소 `.venv`, `python3` 순으로 선택합니다.
- `pre-commit`은 whitespace 오류, conflict marker, `.env`, credential, 생성 package artifact를 거부하고 `dure/src`와 `dure/tests`를 compile합니다.
- `pre-push`는 전체 unit suite, 깨끗한 wheel build, 독립 migration smoke test와 `pyproject.toml`·`setup.py`·runtime·Debian changelog의 버전 동기화를 확인합니다.
- `core.hooksPath`는 Git clone에 포함되지 않으므로 각 clone에서 위 설정을 한 번 실행해야 합니다. WSL checkout에서는 WSL Git과 Linux Python을 사용합니다.

긴급 상황이 아닌 한 hook을 우회하지 않습니다. 우회했다면 누락된 검증을 즉시 실행하고 이유를 남깁니다.

## Pull request와 push CI

`.github/workflows/ci.yml`은 `main` 대상 pull request, `main`과 `version/**` push에서 읽기 전용으로 실행됩니다. Python 3.10·3.12의 compile, unit test, changed-diff whitespace 검사, version 동기화, SQLite migration, wheel build와 Debian package smoke test를 실행합니다. 공개 PR에는 production signing key, Pages environment, GPU runner 또는 model credential을 제공하지 않습니다.

workflow를 merge하고 첫 성공 run이 생긴 뒤 `main` ruleset에서 안정적인 `Required CI` check를 필수로 설정합니다. Pull request, 최신 base 반영 또는 merge queue, review conversation 해결, force-push와 branch 삭제 금지도 함께 요구합니다. required workflow에 `paths-ignore`를 추가하지 않아 check가 skip·pending 상태로 남지 않게 합니다.

## 스키마와 릴리스 변경

schema 변경마다 `src/dure/control/migrations/versions/` 아래에 새 Alembic revision을 만듭니다. 출시된 revision은 수정하지 않으며 새 database와 기존 database 모두에서 `dure-server --migrate`를 검증합니다.

현재 단일 Alembic 계보는 `0006 → 0007 → 0008 → 0009 → 0010 → 0011 → 0012 → 0013 → 0014 → 0015`입니다. `0006`은 배포 operation·노드별 시도·`verified_at`, `0007`은 불변 아티팩트 매니페스트·청크 레지스트리, `0008`은 중앙 모델·이미지 준비 계획과 시도, `0009`는 `STAGE` variant와 신뢰 빌더 증적, `0010`은 준비 시도의 nullable `download_progress`, 노드 아티팩트 캐시 상태와 append-only 이벤트를 추가합니다. `0011`은 placement profile lifecycle과 폐쇄형 실행 설정, `0012`는 exact GPU-bound qualification evidence, `0013`은 불변 Fleet recommendation, `0014`는 수락된 Fleet의 원자적 node/GPU reservation, `0015`는 Fleet별 deployment runtime 상태를 영속합니다. 마이그레이션 테스트는 새 DB와 직전 head 업그레이드, single head, ORM metadata 일치를 모두 검증해야 합니다.

v0.4.18의 3×24GiB harness는 root-owned local 설정과 structured stdout만 사용하며 validation run이나
GPU memory를 중앙 DB에 기록하지 않습니다. 따라서 이 변경에는 빈 `0016` revision을 만들지 않습니다.
향후 controller가 acceptance evidence를 release gate로 저장하려면 새 `0016`에서 append-only evidence
schema를 추가하고, closed field·새 DB·`0015`에서의 upgrade·파괴적 downgrade 거부를 함께 테스트합니다.
raw log, credential, command, Docker argument, mount, host path는 그 evidence에 저장하지 않습니다.

`artifact_cache_events`의 추가 전용 계약은 서비스 관례에만 의존하지 않습니다. `Base.metadata.create_all`과 `0010` upgrade 모두 SQLite·PostgreSQL에 `UPDATE`·`DELETE` 거부 트리거를 설치해야 합니다. SQLite는 숨은 `rowid` 교체 우회를 없앤 `WITHOUT ROWID`와 충돌 `INSERT OR REPLACE` 거부 트리거까지 실제 raw SQL·ORM 변경으로 시험합니다. PostgreSQL은 `TRUNCATE` 문장 트리거도 생성하지만 단위 검증 범위는 생성 DDL과 downgrade 정리의 mock/compile 검사이며 실제 PostgreSQL 실행이나 부하 시험이 아닙니다. 이 장치는 권한 있는 DDL까지 막는 WORM이 아니므로 이를 수용 증적으로 과장하지 않습니다.

downgrade 테스트는 기존 배포·계획·task 보존뿐 아니라 안전 거부 경로도 검증해야 합니다. `active_lineage_id IS NOT NULL`인 operation, 상태가 `PREPARED`·`QUEUED`·`RUNNING`인 operation 또는 operation에 연결된 `QUEUED`·`RUNNING` task가 하나라도 있으면 `0006 → 0005` downgrade는 실패해야 합니다. `0010 → 0009`는 `node_artifact_caches`·`artifact_cache_events`에 데이터가 하나라도 있거나 준비 시도에 SQL `NULL`·JSON `null`이 아닌 `download_progress`가 있으면 파괴적 downgrade를 거부해야 합니다. 테스트나 운영 중 이 검사를 우회하려고 행이나 제약을 직접 삭제하지 않습니다.

마이그레이션·진입점·패키징 변경에는 다음 검사도 수행합니다.

```bash
dure-server --database-url sqlite:////tmp/dure-migration-check.db --migrate
python3 -m pip wheel . --no-deps --no-build-isolation -w /tmp/dure-wheel-check
```

릴리스 전에는 `pyproject.toml`, `setup.py`, `src/dure/__init__.py`, `debian/changelog`의 버전을 일치시킵니다. `scripts/check_version_sync.py`, `scripts/build-deb.sh`, wheel build와 migration smoke를 확인하고 Debian version과 정확히 같은 `v<version>` tag를 push해야 서명된 APT workflow가 실행됩니다. `version/<semver>` branch, draft PR, `UNRELEASED` changelog 항목과 version synchronization은 release publish 권한이 아니며, tag·GitHub Release·APT publish는 사용자의 명시적 요청 전에는 수행하지 않습니다.

## 모델 선택 기능 개발 규칙

모델 추천은 순수하고 결정론적인 계획기여야 합니다. Codex 진단, 현재 시간, 프로필 입력 순서, 임의 네트워크 호출에 따라 결과가 바뀌면 안 됩니다.

- 명시적 `--model`과 기존 계획 JSON 호환성을 유지합니다.
- 신규 프로필 필드는 선택적 기본값을 가져 구 에이전트 프로필을 읽을 수 있어야 합니다.
- 선택 메타데이터는 기존 `DeploymentPlan.model` 전송 스키마에 임의 필드를 추가하지 않습니다.
- 0.3.12 중앙 제어면과 migration을 먼저, 에이전트를 나중에 업그레이드합니다. 0.3.12 미만 Agent의 성공 결과는 전체 세대의 `verified_at` 롤백 증거가 될 수 없어야 합니다.
- 레지스트리 릴리스, 리비전, 매니페스트, 런타임 이미지 다이제스트를 픽스처로 고정합니다.
- 승인됨·온라인·최신 노드, VRAM 경계, 디스크·런타임·네트워크 거부, 프로필 순서 치환, 오래된 인벤토리, 추천 멱등성을 모두 테스트합니다.
- 추천 스냅샷과 인벤토리 지문은 같은 정규화 함수에서 만들어야 하며, 수락 재검사는 새 추천이나 실행 객체를 저장하지 않아야 합니다.
- 추천 수락 테스트는 실제 `PASSED` 증적과 승격 게이트를 거친 `ACTIVE` 릴리스를 사용하고, 상태 직접 변경으로 게이트를 우회하지 않습니다.
- 추천 수락으로 만든 세대는 다운로드·pull 플래그가 항상 거짓이어야 하며, 명시적 apply 전에는 task나 호스트 변경이 없어야 합니다.
- `APPLY`·`VERIFY` operation은 전체 상태, 노드별 상태와 task 시도 번호를 함께 검증합니다. 성공 경로뿐 아니라 부분 실패, 취소, 실패 노드 재시도와 이전 시도의 늦은 claim·완료 거부를 테스트합니다.
- `verified_at`은 계획의 정확한 전체 노드가 모두 성공하고 legacy Agent는 0.3.12 이상, `VLLM_RAY_PP_V1` Agent는 0.3.18 이상, `STAGE` Agent는 0.3.19 이상이며 엄격한 rank·API 검증까지 통과할 때만 기록되는지 검증합니다. 부분 노드와 전체 배정 집합을 충족하지 않는 Ray head 전용 API 검증은 증거를 만들면 안 됩니다.
- `STAGE` 테스트는 exact `VALIDATED` digest만 선택하는지, node UUID↔PP rank 매니페스트가 교환되면 거부하는지, 복합 cache identity·marker·정규 sidecar·전체 tree가 바뀌면 Docker 호출 전에 실패하는지, `FULL_SNAPSHOT`으로 자동 fallback하지 않는지를 확인합니다. 일반 probe는 대용량 stage를 매 heartbeat마다 재해시하지 않으므로 관측 identity를 권위 있는 `READY` 증거로 사용하면 안 됩니다.
- 롤백 테스트는 기본 준비에서 task가 0개인지, `apply=true`에서만 시작하는지, 직접 직전 검증 세대·동일 전체 노드·동일 실행 토폴로지·승인·온라인·다이제스트 이미지 조건을 모두 확인합니다. 엄격한 backend의 실행 토폴로지는 노드·GPU·role·rank·expected runtime rank·runtime address와 backend·vLLM·TP/PP·Ray·network 결합이며 모델·revision·layer 범위·매니페스트·variant·cache kind는 대상 exact 게이트를 통과하면 달라도 됩니다. legacy에서는 layer 범위 비교를 유지합니다.
- 롤백 단계는 `STOP_SOURCE → START_TARGET(serve=false) → VERIFY_TARGET`과 선택적 `START_API → VERIFY_API`를 순서대로 검증하고, 각 단계의 모든 노드가 성공하기 전에 다음 task를 만들면 안 됩니다.
- 호스트 명령 테스트는 `dure.deployment`, `dure.generation`, `dure.node`가 모두 정확히 일치하는 컨테이너만 조작하는지 확인합니다. `dure.node`가 없는 레거시 호환은 배포와 세대가 모두 일치할 때만 허용하고 다른 레이블 불일치는 항상 거부합니다.
- 롤백 task는 모델 다운로드와 이미지 pull을 허용하지 않으며 실제 GPU, Docker 데몬, PostgreSQL이나 인터넷 없이 단위 테스트할 수 있어야 합니다.

## 아티팩트 캐시 기능 검증 규칙

- `READY`는 현재 준비 시도의 최신 성공만 만들 수 있어야 합니다. 이전 task의 늦은 보고, 불완전 probe와 `PRESENT` 관측이 `STALE`·`MISSING`·`CORRUPT`·`QUARANTINED`를 치유하지 못하는지 검증합니다. 부재로 인한 `MISSING` 전환은 `scan_complete=true`인 폐쇄형 전체 조사만 생성해야 합니다.
- 추천·수락·준비 preview는 task 0개를 유지하고, 명시적 준비 apply만 다운로드·image pull task를 만들어야 합니다. 배포 apply·verify와 rollback target은 노드별 exact cache identity, 현재 모델 시도, 최신 image digest를 다시 검사하고 하나라도 준비되지 않으면 Docker 호출 전에 거부해야 합니다. 롤백은 네트워크를 사용해 캐시나 이미지를 복구하면 안 됩니다.
- 자동 `BENCHMARK`는 exact `FULL_SNAPSHOT`, verification version 1, 단일 GPU `dure-benchmark` 계약만 소비해야 합니다. `STAGE`, 다중 노드·다중 GPU, `VLLM_RAY_PP_V1` 배포 계약을 자동 벤치마크에 주입하는 성공 경로를 만들지 않습니다. 지원하지 않는 cache kind·backend를 Docker 실행 전에 거부하는 테스트를 유지합니다.
- `artifact-cache list/show/verify`는 읽기 전용이고 `verify`는 파일 전체 재해시나 상태 치유를 하지 않는지 검증합니다. quarantine는 기본 preview에서 task와 이벤트가 0개이고, `--apply`에서도 현재·직전 세대와 활성 준비·배포·벤치마크 참조를 모두 제거한 exact 비활성 final 하나만 원자 이동하는지 검증합니다. 자동 삭제, target 덮어쓰기, copy fallback은 성공 경로로 허용하지 않습니다.
- `tests/test_artifact_cache_lifecycle.py`, `tests/test_artifact_preparation_control.py`, `tests/test_artifact_distribution_e2e.py`, `tests/test_cache_quarantine_api.py`, `tests/test_benchmark_runtime.py`, `tests/test_deployment_rollout.py`, `tests/test_migrations.py`에서 정상 경로와 partial failure·retry·corruption·stale report·wrong rank·variant revoke·안전 downgrade 거부를 함께 검증합니다. 통합 테스트는 실제 Agent 준비기와 중앙 수명 주기를 연결하되 가짜 호스트 명령을 사용하므로, 이를 실제 GPU·Docker·PostgreSQL·네트워크 수용 증거로 보고하지 않습니다.

GPU 수용 검사는 보호된 환경에서만 실행하며 공개 CI나 신뢰할 수 없는 PR에 GPU 실행기·모델 자격 증명·서명 비밀값을 노출하지 않습니다. `acceptance-vllm-stage-ray-pp.py`를 포함한 opt-in harness의 기본 `NOT_RUN(77)`은 성공으로 바꾸지 않습니다. 자세한 기준은 [benchmarking.md](benchmarking.md)를 참고합니다.

소스 checkout이나 editable 설치에서 폐쇄형 `BENCHMARK` 작업을 직접 시험할 때는 실행 중인 코드의 40~64자리 커밋 해시를 `DURE_BUILD_COMMIT`으로 명시합니다. 공식 Debian 빌드는 이 값을 `/usr/share/dure/build-commit`에 설치합니다. 값이 없거나 작업의 `dure_commit`과 다르면 Agent는 GPU 조사나 컨테이너 실행 전에 작업을 거부합니다.
