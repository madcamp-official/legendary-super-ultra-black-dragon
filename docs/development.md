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
git config core.hooksPath .githooks
```

- `pre-commit`은 whitespace 오류, conflict marker, `.env`, credential, 생성 package artifact를 거부하고 Python source를 compile합니다.
- `pre-push`는 전체 unit suite, 깨끗한 wheel build, 독립 migration smoke test를 실행합니다.

긴급 상황이 아닌 한 hook을 우회하지 않습니다. 우회했다면 누락된 검증을 즉시 실행하고 이유를 남깁니다.

## 스키마와 릴리스 변경

schema 변경마다 `src/dure/control/migrations/versions/` 아래에 새 Alembic revision을 만듭니다. 출시된 revision은 수정하지 않으며 새 database와 기존 database 모두에서 `dure-server --migrate`를 검증합니다.

0006은 배포 operation, 노드별 단계·시도 연결과 `verified_at`을 추가합니다. downgrade 테스트는 기존 배포·계획·task 보존뿐 아니라 안전 거부 경로도 검증해야 합니다. `active_lineage_id IS NOT NULL`인 operation, 상태가 `PREPARED`·`QUEUED`·`RUNNING`인 operation 또는 operation에 연결된 `QUEUED`·`RUNNING` task가 하나라도 있으면 0005 downgrade는 실패해야 합니다. 테스트나 운영 중 이 검사를 우회하려고 행이나 제약을 직접 삭제하지 않습니다.

마이그레이션·진입점·패키징 변경에는 다음 검사도 수행합니다.

```bash
dure-server --database-url sqlite:////tmp/dure-migration-check.db --migrate
python3 -m pip wheel . --no-deps --no-build-isolation -w /tmp/dure-wheel-check
```

릴리스 전에는 `pyproject.toml`, `setup.py`, `src/dure/__init__.py`, `debian/changelog`의 버전을 일치시킵니다. `scripts/build-deb.sh`로 로컬 build를 확인하고 Debian version과 정확히 같은 `v<version>` tag를 push해야 서명된 APT workflow가 실행됩니다.

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
- `verified_at`은 계획의 정확한 전체 노드가 모두 성공하고 legacy Agent는 0.3.12 이상, `VLLM_RAY_PP_V1` Agent는 0.3.18 이상이며 엄격한 rank·API 검증까지 통과할 때만 기록되는지 검증합니다. 부분 노드와 전체 배정 집합을 충족하지 않는 Ray head 전용 API 검증은 증거를 만들면 안 됩니다.
- 롤백 테스트는 기본 준비에서 task가 0개인지, `apply=true`에서만 시작하는지, 직접 직전 검증 세대·동일 전체 노드·동일 토폴로지·승인·온라인·다이제스트 이미지 조건을 모두 확인합니다.
- 롤백 단계는 `STOP_SOURCE → START_TARGET(serve=false) → VERIFY_TARGET`과 선택적 `START_API → VERIFY_API`를 순서대로 검증하고, 각 단계의 모든 노드가 성공하기 전에 다음 task를 만들면 안 됩니다.
- 호스트 명령 테스트는 `dure.deployment`, `dure.generation`, `dure.node`가 모두 정확히 일치하는 컨테이너만 조작하는지 확인합니다. `dure.node`가 없는 레거시 호환은 배포와 세대가 모두 일치할 때만 허용하고 다른 레이블 불일치는 항상 거부합니다.
- 롤백 task는 모델 다운로드와 이미지 pull을 허용하지 않으며 실제 GPU, Docker 데몬, PostgreSQL이나 인터넷 없이 단위 테스트할 수 있어야 합니다.

GPU 수용 검사는 보호된 환경에서만 실행하며 공개 CI나 신뢰할 수 없는 PR에 GPU 실행기·모델 자격 증명·서명 비밀값을 노출하지 않습니다. 자세한 기준은 [benchmarking.md](benchmarking.md)를 참고합니다.

소스 checkout이나 editable 설치에서 폐쇄형 `BENCHMARK` 작업을 직접 시험할 때는 실행 중인 코드의 40~64자리 커밋 해시를 `DURE_BUILD_COMMIT`으로 명시합니다. 공식 Debian 빌드는 이 값을 `/usr/share/dure/build-commit`에 설치합니다. 값이 없거나 작업의 `dure_commit`과 다르면 Agent는 GPU 조사나 컨테이너 실행 전에 작업을 거부합니다.
