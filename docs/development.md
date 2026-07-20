# 개발과 릴리스 절차

## 개발 환경

```bash
python3 -m pip install -e '.[test]'
python3 -m unittest discover -v
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

릴리스 전에는 `pyproject.toml`, `setup.py`, `src/dure/__init__.py`, `debian/changelog`의 버전을 일치시킵니다. `scripts/build-deb.sh`로 로컬 build를 확인하고 Debian version과 정확히 같은 `v<version>` tag를 push해야 서명된 APT workflow가 실행됩니다.

## 모델 선택 기능 개발 규칙

모델 추천은 순수하고 결정론적인 계획기여야 합니다. Codex 진단, 현재 시간, 프로필 입력 순서, 임의 네트워크 호출에 따라 결과가 바뀌면 안 됩니다.

- 명시적 `--model`과 기존 계획 JSON 호환성을 유지합니다.
- 신규 프로필 필드는 선택적 기본값을 가져 구 에이전트 프로필을 읽을 수 있어야 합니다.
- 선택 메타데이터는 기존 `DeploymentPlan.model` 전송 스키마에 임의 필드를 추가하지 않습니다.
- 중앙 제어면을 먼저, 에이전트를 나중에 업그레이드합니다.
- 레지스트리 릴리스, 리비전, 매니페스트, 런타임 이미지 다이제스트를 픽스처로 고정합니다.
- 승인됨·온라인·최신 노드, VRAM 경계, 디스크·런타임·네트워크 거부, 프로필 순서 치환, 오래된 인벤토리, 추천 멱등성을 모두 테스트합니다.
- 추천 스냅샷과 인벤토리 지문은 같은 정규화 함수에서 만들어야 하며, 수락 재검사는 새 추천이나 실행 객체를 저장하지 않아야 합니다.
- 추천 수락 테스트는 실제 `PASSED` 증적과 승격 게이트를 거친 `ACTIVE` 릴리스를 사용하고, 상태 직접 변경으로 게이트를 우회하지 않습니다.
- 추천 수락으로 만든 세대는 다운로드·pull 플래그가 항상 거짓이어야 하며, 명시적 apply 전에는 task나 호스트 변경이 없어야 합니다.

GPU 수용 검사는 보호된 환경에서만 실행하며 공개 CI나 신뢰할 수 없는 PR에 GPU 실행기·모델 자격 증명·서명 비밀값을 노출하지 않습니다. 자세한 기준은 [benchmarking.md](benchmarking.md)를 참고합니다.

소스 checkout이나 editable 설치에서 폐쇄형 `BENCHMARK` 작업을 직접 시험할 때는 실행 중인 코드의 40~64자리 커밋 해시를 `DURE_BUILD_COMMIT`으로 명시합니다. 공식 Debian 빌드는 이 값을 `/usr/share/dure/build-commit`에 설치합니다. 값이 없거나 작업의 `dure_commit`과 다르면 Agent는 GPU 조사나 컨테이너 실행 전에 작업을 거부합니다.
