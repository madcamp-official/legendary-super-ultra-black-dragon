# CLI 명령 참조

이 문서는 Dure CLI의 운영상 성격을 빠르게 확인하기 위한 참조서입니다. 설치된 버전의 정확한 인자와 기본값은 항상 `dure --help` 및 `dure <명령> --help`를 기준으로 합니다. 명령 이름과 플래그는 호환성 정책에 따라 달라질 수 있습니다.

## 공통 원칙

| 구분 | 의미 |
| --- | --- |
| 로컬 명령 | 실행한 GPU 노드에서 검사·계획·준비를 수행합니다. Control Plane 없이도 쓸 수 있는 명령이 있습니다. |
| 관리자 명령 | Control Plane API에 인증해 중앙 상태를 조회·변경하거나 Agent task를 만듭니다. |
| preview | `--apply`가 없는 실행입니다. 패키지 설치, Docker 설정 변경, 서비스 재시작, 모델 pull·배포를 하지 않아야 합니다. |
| apply | 운영자가 명시적으로 승인한 실제 변경입니다. 명령별 추가 opt-in과 권한이 필요할 수 있습니다. |
| 권한 | `sudo` 필요 여부는 호스트의 패키지·서비스·Docker 접근 권한과 설치 방식에 따라 달라집니다. root가 필요하지 않은 조회 명령에도 민감한 설정 파일을 읽을 권한은 별도로 필요할 수 있습니다. |

중앙 Controller나 Agent는 원격 SSH로 임의 명령을 실행하지 않습니다. Agent가 수행할 수 있는 작업은 닫힌 task enum에 한정되며, 서버 소유자는 로컬에서 `sudo dure bootstrap --apply`처럼 명시적으로 호스트 준비를 승인합니다.

## 로컬 노드 명령

| 명령 | 주된 목적 | 기본 실행 | 실제 변경 조건·주의 |
| --- | --- | --- | --- |
| `dure bootstrap` | OS, NVIDIA 드라이버, Docker, NVIDIA Container Toolkit, Agent 준비 상태 진단 | 변경 없는 계획·차단 사유 출력 | `sudo dure bootstrap --apply`만 제한된 설치·설정을 수행합니다. Docker 재시작이 필요한데 컨테이너가 있으면 `--allow-docker-restart` 없이는 중단합니다. 드라이버·커널·CUDA 호스트 패키지는 변경하지 않습니다. |
| `dure doctor` | GPU·런타임·네트워크의 읽기 전용 진단 | 읽기 전용 | `--json`, `--output`은 출력 형식·저장 위치만 지정합니다. |
| `dure plan` | 로컬 인벤토리와 배치 프로필에서 결정론적 계획 작성 | 계획 파일 생성 | `--output`은 필수입니다. Docker·모델·서비스를 변경하지 않습니다. `--image` 기본값은 편의값일 뿐 중앙 배포에는 digest-pinned 이미지 계약이 적용됩니다. |
| `dure init` | 계획을 검토하고 로컬 준비·배포 절차 수행 | `--apply` 없이는 preview | `--apply`가 실제 상태 변경 관문입니다. 모델 다운로드는 `--accept-model-download`, image pull은 `--pull`, digest 없는 이미지는 `--allow-unpinned-image`, 기존 Dure 컨테이너 교체는 `--replace`, 서빙은 `--serve`를 각각 명시해야 합니다. |
| `dure status` | 로컬 상태 파일의 배포 상태 조회 | 읽기 전용 | `--state-file`, `--json`으로 대상·형식을 지정합니다. |
| `dure verify` | 계획 및 선택한 API endpoint의 준비 상태 확인 | 읽기 전용 검증 | 모델을 설치하거나 컨테이너를 시작하지 않습니다. 대상 API에 네트워크 요청을 보낼 수 있습니다. |
| `dure join` | 노드를 Control Plane에 등록 | pending 등록과 Agent 설정 | `--server`, `--insecure` 또는 설정 값을 사용합니다. 성공해도 노드는 `pending`이며 운영자 승인 전 task를 받지 못합니다. |
| `dure unjoin` | 현재 노드의 등록 해제 | Agent 중지·등록 자격 증명 제거 | 정확한 Dure 배포 label을 가진 컨테이너만 대상으로 정리합니다. 캐시·로그의 보존·삭제 판단은 [노드 수명주기](node-lifecycle.md)를 따릅니다. |

`bootstrap`, `doctor`의 JSON은 자동화용 진단 결과입니다. JSON에 credential, 관리자 토큰, model token을 넣거나 로그에 남겨서는 안 됩니다.

## 관리자 연결과 인증

관리자 명령은 다음 연결 정보를 사용합니다.

```bash
dure --server https://controller.example --token "$DURE_ADMIN_TOKEN" admin nodes
```

`--server`, `--token`, `--env-file`을 `admin` 앞에 둡니다. 우선순위와 파일 권한은 [설정 참조서](configuration-reference.md)를 따릅니다. 관리자 token과 enrollment credential은 표준 출력·셸 히스토리·Issue·PR에 남기지 않습니다.

## 관리자 명령군

| 명령군 | 예시 | 중앙 상태 | GPU 노드 변경 |
| --- | --- | --- | --- |
| 조회 | `admin nodes`, `node show`, `tasks`, `diagnose --no-refresh`, `artifact-cache list/show`, `fleet show/status`, `deployment show/generations/preparation`, `recommendation show` | 조회 | 없음. 단, `diagnose`에서 refresh를 허용하면 probe task가 생성될 수 있습니다. |
| 등록·승인 | `enrollment create`, `node approve`, `admin unjoin` | enrollment·노드 상태 변경 | `admin unjoin`은 대상 Agent에 등록 해제 task를 보낼 수 있습니다. |
| 아티팩트·증적 | `artifact-manifest register/show`, `artifact-cache verify`, `artifact-cache quarantine --apply` | 매니페스트·캐시 상태 변경 가능 | quarantine은 `--apply`에서 대상 cache의 격리 작업을 요청합니다. |
| 추천·Fleet | `recommendation show/accept`, `fleet recommend/show/accept/status` | 추천·Fleet의 생성·수락 | 추천·수락만으로 모델 다운로드나 컨테이너 시작은 일어나지 않습니다. |
| 준비·적용 | `fleet prepare`, `fleet apply`, `deployment prepare`, `deployment apply/start/stop/restart`, `deployment rollback`, `activate` | deployment generation·task 상태 변경 | Agent가 명시된 닫힌 작업만 수행합니다. 적용·롤백·활성화에는 해당 명령의 `--apply`와 승인된 상태가 필요합니다. |
| 검증·운영 | `probe`, `verify`, `deployment recommend/create` | 증적·추천·작업 상태 변경 가능 | probe·verify는 Agent의 GPU·네트워크·모델 검증 task를 만들 수 있습니다. 검증 범위와 비용을 먼저 확인합니다. |
| credential | `credential revoke`, `credential rotate` | credential 상태 변경 | rotate 결과의 새 비밀은 한 번만 출력될 수 있습니다. 즉시 안전한 전달 채널로 옮기고 Agent 설정 갱신·재시작·heartbeat를 확인합니다. |

`admin deployment`의 세부 하위 명령과 허용 상태 전이는 [배포·generation 운영](operations.md), [Agent 운영](agent-operations.md), [API 계약](api-contract.md)을 함께 확인합니다. 다중 노드 Fleet은 수락 시 노드·GPU 예약을 원자적으로 검사하며, 일부 노드만 임의로 적용하지 않습니다.

## 안전한 실행 순서

1. 대상 노드에서 `dure doctor --json`과 `dure bootstrap`을 실행해 차단 사유를 확인합니다.
2. `dure join` 뒤 중앙에서 노드를 검토·승인합니다.
3. 추천·qualification·증적을 확인하고, `recommend` 또는 `fleet recommend` 결과를 검토합니다.
4. 필요한 경우에만 준비·적용 명령과 `--apply`를 명시합니다.
5. 적용 후 `status`, `verify`, Agent heartbeat와 task 결과를 확인합니다. 실패하면 [관측·장애 대응 운영 절차](observability.md)와 [복구 절차](disaster-recovery.md)를 따릅니다.

공개 인터넷에 Ray GCS, dashboard, worker 포트 또는 vLLM API를 직접 노출하지 않습니다. 포트·방화벽·NCCL 인터페이스는 [네트워크 운영](networking.md)을 기준으로 설정합니다.
