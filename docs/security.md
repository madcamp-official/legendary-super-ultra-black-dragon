# 보안 모델과 보안 강화 계획

## 보호 대상

- admin과 node bearer credential
- host root 권한과 Docker 제어권
- model·stage artifact와 immutable exporter build·runtime image identity
- deployment topology, task 이력, node inventory
- vLLM이 처리하는 prompt와 생성 데이터

## 주요 위협과 현재 통제

| 위협 | 현재 통제 | 남은 과제 |
|---|---|---|
| 권한 없는 노드의 작업 수신 | 새 join은 운영자 승인 전 pending | join rate limit과 네트워크 제한 |
| 탈취된 node credential | 노드별 hash 저장과 개별 revoke | mTLS와 자동 rotation |
| 중앙 제어면의 원격 셸화 | 폐쇄형 작업 열거형, 검증된 페이로드와 고정 `BENCHMARK` 실행기 | 호스트 작업 권한 분리 |
| 이미지 치환 | 중앙 계획은 OCI 다이제스트 요구, 준비 작업은 exact digest inspect·필요 시 pull·재inspect | 이미지 서명과 출처 검증 |
| 모델 아티팩트 변경 | 리비전 고정 정책, 불변 정규 매니페스트, 중앙 준비의 콘텐츠 주소 청크·파일 SHA-256 재검증과 marker-last 캐시 활성화, 추천 apply의 exact evidence 게이트 | 게시자 서명과 provenance 검증 |
| stage 출력 치환·rank 혼합 | source·runtime·exporter·토폴로지와 rank별 매니페스트를 결합한 variant identity, 완전한 rank 집합 검사와 실제 GPU export/load 승격 게이트 | 서명된 빌더 provenance와 투명성 로그 |
| stage builder 공급망·코드 실행 | digest 고정 별도 OCI 환경, 제한된 vLLM 0.9.0 계약, remote code·LoRA·MoE·멀티모달·임의 아키텍처 거부 | builder 이미지 서명과 재현 빌드 검증 |
| 다중 노드 rank 오결합 | `VLLM_RAY_PP_V1`, 서버 UUID, 고유 RFC1918 주소, head 우선·worker 주소 정렬, 전체 계획·Ray topology 재검증 | 실제 2·3노드 GPU 수용 검사와 서명된 증적 provenance |
| 벤치마크 증적 바꿔치기 | 릴리스·배치·정확한 정렬 노드 UUID 조합·현재 프로필 지문과 고정 아티팩트·런타임 식별자 결합, Agent·서버의 폐쇄형 결과 재검증, 중앙 추천의 24시간 TTL과 최신 결과 검사 | 서명된 Agent 결과와 원본 증적 provenance |
| 작업 재생 | 임대, 세대 검사, 로컬 완료 저널, operation 단계·노드·시도 번호 펜싱 | 서명된 봉투와 암호학적 펜싱 토큰 |
| 동시 변경 | 노드 행 잠금, 한 개의 활성 임대와 계보당 한 개의 활성 변경 | PostgreSQL 동시성 부하 시험 |
| 컨테이너 오조작 | 배포·세대·노드와 엄격한 backend·pipeline rank·runtime rank·component 레이블의 조회 후 재검사 | 격리와 실제 Docker 경쟁 조건 검토 |
| 잘못된 롤백 대상 | 서버가 직접 직전 검증 세대를 선택하고 전체 노드·토폴로지 재검사 | 실제 다중 노드 복구 수용 검사 |
| 공개 Ray 노출 | 사설망 사용 문서화 | WireGuard와 firewall 검증 자동화 |
| join endpoint 남용 | pending 노드에는 작업 권한 없음 | rate limit, quota, audit alert |
| host 운영자의 prompt 관찰 | community workload는 비기밀로 선언 | confidential-computing 또는 private pool |

## 운영 요구 사항

- 개발 목적이 아닌 모든 에이전트 연결에는 HTTPS를 사용합니다.
- `DURE_ADMIN_TOKEN`, 데이터베이스 자격 증명, APT 서명 키, 모델 자격 증명을 Git 밖에 보관합니다.
- 가능하면 join과 중앙 제어면 종단점을 신뢰된 LAN 또는 사설 오버레이로 제한합니다.
- 노드를 승인하기 전 호스트명, GPU 인벤토리, 주소, 소유자를 검토합니다.
- 배포 이미지와 모델 리비전을 고정하고 Ray 포드 전체에서 같은 검증 런타임을 사용합니다.
- `VLLM_RAY_PP_V1`의 GCS 6379와 worker 20000-21000은 정확한 사설 포드 주소 사이에만 허용하고 API 8000은 loopback에 둡니다.
- 프롬프트와 자격 증명을 기록하지 않고 메타데이터와 오류만 수집합니다.
- `dure admin diagnose`는 명시적 외부 처리입니다. 선택된 인벤토리가 운영자 컴퓨터의 Codex 제공자로 전송될 수 있지만 자격 증명, 컨테이너 환경 변수·명령, 프롬프트는 전송하지 않습니다.
- PostgreSQL 백업, 자격 증명 폐기, 복구 절차를 실제로 검증합니다.

## 아티팩트 매니페스트의 신뢰 경계

중앙 제어면은 모델 아티팩트에 결합된 불변 정규 매니페스트와 상대 일반 파일·청크의 SHA-256, 크기와 연결 범위를 저장합니다. 서버가 정규 JSON 다이제스트, 경로, 파일을 빈틈없이 덮는 청크 순서와 총계를 검증하므로 우발적인 입력 순서 차이, 경로 탈출, 겹침·누락과 일관되지 않은 크기를 DB에 등록할 수 없습니다. 같은 청크는 다이제스트 기준으로 공유하며, 같은 매니페스트의 재등록은 멱등적입니다.

이 통제는 등록 문서의 구조와 향후 받은 바이트의 기대값을 고정할 뿐 게시자 신원을 인증하지 않습니다. 공격자가 모델 파일과 매니페스트를 함께 바꾸면 새 SHA-256을 만들 수 있으므로 해시 일치는 게시자 서명, 투명성 로그나 신뢰할 수 있는 공급망 provenance가 아닙니다. 현재 등록 서비스는 실제 모델 파일을 읽거나 다운로드하지 않아 등록된 파일 해시를 독립 검증하지도 않습니다.

- 파일 항목은 루트 기준 상대 경로의 일반 파일만 허용하며 절대 경로, `..`, 심볼릭 링크, 장치와 정규화 중복 경로를 거부합니다.
- 등록 요청은 원본 접근 토큰, 쿠키, 자격 증명, 임의 헤더, 명령, 환경 변수, 호스트 경로나 마운트를 받지 않습니다.
- legacy 아티팩트에 검증된 매니페스트를 합성하지 않습니다. 정규 매니페스트가 없는 아티팩트는 미등록 상태로 남습니다.
- 등록·조회는 중앙 DB만 변경하며 Agent 작업, 다운로드, 이미지 내려받기, 캐시 파일, 컨테이너나 배포를 만들거나 바꾸지 않습니다.
- 노드 로컬 준비기는 신뢰 HTTPS origin에서 받은 청크와 조립 파일의 SHA-256을 실제로 검사하지만, 중앙 등록 자체는 여전히 파일을 읽지 않습니다.

게시자 서명과 provenance 검증이 구현될 때까지 운영자는 신뢰된 오프라인 작성 환경과 관리자 인증 경계 안에서만 매니페스트를 생성·등록해야 합니다.

## stage artifact와 오프라인 builder의 신뢰 경계

0.3.17의 stage 계층은 정규 매니페스트로 검증된 `FULL_SNAPSHOT`을 digest 고정 builder runtime에서 pipeline rank별 vLLM `sharded_state`로 내보냅니다. 지원 범위는 정확히 vLLM 0.9.0, V0 executor, `Qwen2ForCausalLM` AWQ, `TP=1`입니다. `trust_remote_code=false`를 강제하고 `auto_map`, Python 모델 코드, LoRA·adapter, MoE, 멀티모달과 임의 아키텍처를 거부합니다.

vLLM의 sharded-state 파일명 rank는 TP rank입니다. `TP=1`, `PP>1`의 모든 worker가 공용 디렉터리에 쓰면 같은 `model-rank-0-*` 이름이 충돌하거나 덮어써질 수 있습니다. builder는 worker별 pipeline rank를 확인해 `stages/<pp-rank>`로 출력 경계를 분리합니다. Dure의 계획 레이어 범위로 원본 가중치 파일을 임의 절단하지 않습니다.

variant identity에는 source manifest, runtime OCI digest, vLLM 버전, exporter build digest, 아키텍처·양자화, TP·PP, loader 형식과 rank 정렬 stage manifest가 들어갑니다. 등록은 `0..PP-1` rank를 완전한 집합으로 처리하며 누락·중복·범위 밖 rank, topology 불일치와 같은 고정 입력에서 달라진 출력을 거부합니다. 각 stage도 기존 정규 경로·일반 파일·청크 SHA-256 계약을 따릅니다.

등록된 variant는 `DRAFT`이며 등록 자체는 GPU load 가능성 증명이 아닙니다. synthetic 검사는 구조적 거부 경로를 확인하지만 승격 권한이 없고, 정확한 identity에서 실제 export와 load가 모두 성공한 최신 `GPU_EXPORT_LOAD/PASSED`만 `VALIDATED` 전환을 허용합니다. 전제 조건이 없어 실행하지 못한 `NOT_RUN`과 `FAILED`는 DRAFT를 승격할 수 없습니다. 새 canonical UUIDv4 validation run 증적은 `DRAFT`에서만 추가합니다. 이미 등록한 동일 run의 정확한 재전송은 `VALIDATED`나 `REVOKED` 뒤에도 멱등 반환하지만, 두 상태에서 새 run을 추가하는 요청은 거부합니다. 검증 뒤 신뢰 문제가 발견되면 운영자가 영향 범위를 검토해 명시적으로 `REVOKED`로 닫고 수정된 계약은 새 `DRAFT` variant에서 검증합니다. builder GPU acceptance는 `PP=1`, 별도 분산 runtime acceptance는 준비된 `PP=2/3`의 load·최소 추론을 검사하며 실제 수행하지 않은 결과를 `PASSED`로 취급해서는 안 됩니다.

등록·증적·상태 전이만으로는 다운로드, P2P 전송, 캐시 활성화, Agent task, Docker 실행이나 기존 배포 변경이 일어나지 않습니다. 0.3.19의 별도 준비 요청이 exact `VALIDATED` digest를 명시한 경우에만 rank별 task가 생깁니다. 각 Agent는 source·variant·runtime·topology·rank·tensor-key 전체의 복합 cache identity를 계산하고, 전체 파일·marker 재해시와 no-replace 활성화 뒤에만 이를 사용합니다. 실패 또는 철회도 실행 중인 이전 배포를 자동 중지하지 않으며 `STAGE`에서 `FULL_SNAPSHOT`으로 자동 fallback하지 않습니다.

vLLM·PyTorch·safetensors·CUDA 계열 heavy dependency는 기본 Debian Agent 패키지에 넣지 않습니다. root Agent와 중앙 서버를 builder로 재사용하지 않고 네트워크·입출력 경계를 통제한 별도 digest 고정 환경에서만 실행합니다. 이 분리는 기본 설치의 공격 표면을 줄이지만 builder 이미지 자체가 신뢰된다는 암호학적 증명은 아닙니다.

SHA-256과 OCI digest는 선택한 바이트의 동일성을 고정하지만 게시자 신원, 모델 안전성, builder 작성자나 공급망 provenance를 증명하지 않습니다. 관리자 인증 경계, 원본·라이선스 검토와 별도 이미지 서명 정책이 계속 필요합니다. 상세 운영 계약은 [stage artifact 문서](stage-artifacts.md)를 따릅니다.

## 폐쇄형 다중 노드 실행의 신뢰 경계

`VLLM_RAY_PP_V1`은 정확히 vLLM 0.9.0 V0 Ray, `TP=1`, `PP=2/3`, 노드별 정상 GPU 한 장과 검증된 `FULL_SNAPSHOT` 또는 exact rank `STAGE`를 지원합니다. 새 backend는 별도 필드가 없는 기존 계획 JSON과 legacy 실행에 영향을 주지 않습니다. 반대로 엄격한 필드 일부만 legacy 계획에 섞거나 알 수 없는 backend·vLLM·cache kind를 지정하면 실행 전에 거부합니다.

중앙 계획은 hostname을 실행 identity로 사용하지 않고 서버가 발급한 canonical UUID를 직접 배정합니다. head는 rank 0으로 고정하고 worker는 중복 없는 canonical RFC1918 IPv4 문자열 순으로 정렬합니다. 계획의 rank·layer 범위·노드·주소 집합은 빈틈없이 연속이어야 하며, 각 노드 Agent는 현재 probe에서 같은 UUID, `default_interface_addresses`에 정확히 결합된 계획 주소, 모든 노드에서 같은 기본 interface, 정상 GPU 정확히 한 장, cache kind에 맞는 exact marker와 Docker NVIDIA runtime을 다시 확인합니다. 비중지 작업은 전체 배정 집합과 0.3.18 이상 Agent를 요구하고 `STAGE`는 0.3.19 이상이어야 합니다.

Ray 실행 입력도 폐쇄돼 있습니다. GCS `6379`, worker `20000-21000`, API `127.0.0.1:8000`, `--node-ip-address`, `VLLM_HOST_IP`, Ray backend와 TP/PP 값은 코드의 고정 계약에서 생성합니다. 중앙 task가 임의 명령, 포트, Docker 인자, 환경 변수, mount나 host path를 주입할 수 없습니다. 다만 host network를 사용하는 Ray 컨테이너는 별도 network namespace 격리를 제공하지 않으므로, RFC1918 검사만으로 보안을 충족한다고 보아서는 안 됩니다. host firewall과 사설 overlay가 실제 접근 제어 경계입니다.

엄격한 컨테이너는 `dure.deployment`, `dure.generation`, `dure.node`, `dure.backend`, `dure.pipeline-rank`, `dure.runtime-rank`, `dure.component`가 모두 일치해야 Dure가 시작·검증·중지·재시작 대상으로 인정합니다. 레이블 누락·교환·중복과 다른 component 컨테이너는 이름이 같아도 조작하지 않습니다. 이 검사는 로컬 root나 Docker daemon을 장악한 공격자를 방어하지는 않습니다.

`pipeline-rank-contract`의 증명 범위는 제한돼 있습니다. Dure는 컨테이너 안의 vLLM 버전과 Ray가 보고한 살아 있는 노드 주소·GPU 수, 주소별 `dure_node_<uuidhex>` custom resource를 직접 확인하고, API 시작 뒤 검사에서는 worker actor topology도 요구합니다. 이를 vLLM 0.9.0 소스에 고정된 worker 정렬 계약과 결합해 계획의 rank binding을 다시 계산합니다. Ray 상태가 vLLM 내부 pipeline rank 숫자를 직접 공개하는 것은 아니므로 이 결과는 **소스 고정 계약과 간접 topology 증적**입니다. 다른 vLLM 버전, actor 구현이나 정렬 규칙에 일반화할 수 없으며, 직접 runtime rank 관측 또는 악성 worker 부재 증명으로 표현해서는 안 됩니다.

엄격한 컨테이너의 `dure.runtime-contract` SHA-256 레이블은 이미지·모델 mount·GPU·host network·entrypoint·고정 환경과 명령의 drift를 시작·재사용·readiness에서 차단합니다. 이 레이블은 host root 공격에 대한 원격 attestation은 아닙니다. 긴급 `STOP`은 캐시·준비 경로 또는 이 레이블이 손상돼도 정확한 배포·세대·노드·backend·rank·component 레이블로 대상을 한정해 실행하며 runtime-contract 값을 신뢰 근거로 요구하지 않습니다.

별도의 GPU harness는 신뢰된 2·3노드 환경에서 실제 Ray executor, worker 배치, 분산 load와 최소 추론을 확인합니다. opt-in 전이나 설정·runtime·모델 전제 부족은 `NOT_RUN`·77이고 실제 실행 시작 뒤 오류는 `FAILED`입니다. 설정은 `/etc/dure/acceptance-vllm-ray-pp-v1.json`, 모델 mount는 `/models/model`로 고정되며 command, Docker 인자, 임의 환경 변수 묶음과 host path를 입력받지 않습니다. harness는 Ray custom resource를 통해 주소와 Dure UUID를 대조하지만 설정의 runtime image digest는 선언값이며 현재 프로세스의 OCI digest를 자체 증명하지 않습니다. 신뢰된 digest 고정 wrapper가 실제 실행 문맥과 중앙 계획을 대조해야 유효하고 controller의 노드별 증적을 대체하지 않습니다. 이 harness도 결과 서명, 원격 attestation, driver 무결성이나 host root 침해를 증명하지 않습니다.

실패 시 새 단계는 닫히고 사전 검사 전이라면 기존 세대를 변경하지 않습니다. 실행 전환이 시작된 뒤에는 이전 세대가 계속 실행된다고 가정하지 않고 명시적 상태 확인과 롤백을 수행합니다. 반복 실패 노드는 credential revoke로 작업 수신을 격리할 수 있지만 캐시 파일을 자동 `QUARANTINED` 상태로 옮기는 기능은 아직 없습니다. Dure는 NVIDIA driver를 설치·변경하지 않으며, driver·CUDA·GPU 오류는 운영자가 노드를 격리하고 지원 조합으로 수동 복구해야 합니다. 자동 failover·자동 rollback·자동 cache 삭제는 이 신뢰 경계에 포함되지 않습니다.

## 콘텐츠 주소 캐시의 신뢰 경계

production 기본값에서 노드 준비기는 Dure 소유 고정 루트와 매니페스트 digest로 CAS·staging·final 경로를 계산합니다. 내부 저장소 생성자는 테스트·로컬 임베딩용 루트 override를 허용하지만 원격 task payload와 연결할 수 없습니다. Agent는 `/etc/dure/agent.json`의 root 전용 `artifact_origin`에서만 `TrustedHTTPSOrigin`을 구성합니다. 최초 HTTPS object URL은 이 객체와 청크 SHA-256으로 만들고 userinfo·query·fragment와 허용되지 않은 redirect host·port를 거부합니다. 허용 host의 redirect path는 신뢰 origin 경계에 속합니다. 모호한 길이·범위, 압축·chunked 응답도 거부합니다. 원본 token, cookie, 임의 header와 raw URL은 task payload·매니페스트·중앙 DB·시도 저널·결과에 저장하지 않습니다. 현재 전송기는 인증 token·cookie·사용자 지정 header를 지원하지 않으므로 별도 credential 없이 접근 가능한 신뢰 origin이 필요합니다.

각 청크와 매니페스트에는 프로세스 잠금을 사용합니다. 기존 CAS는 크기·소유권·link 수·쓰기 권한·전체 SHA-256이 모두 맞을 때만 재사용합니다. 부분 다운로드는 이어받은 뒤 전체 청크 SHA-256을 다시 계산하고, 부분 조립은 재개할 prefix를 검증된 CAS 바이트와 직접 비교합니다. 파일·디렉터리 `fsync`, 전체 트리 검사와 v2 marker 기록 뒤 Linux no-replace rename을 사용하므로 marker 없는 staging이나 검증되지 않은 final을 READY로 해석하지 않습니다.

실패 시의 기본 정책은 보존과 차단입니다.

- 이미 게시된 CAS 청크, 비일시 staging 항목, marker와 final의 오염·충돌은 덮어쓰거나 자동 삭제하지 않습니다. 다운로드 응답 거부나 digest 불일치가 난 transient 청크 `.part`는 안전하게 0바이트로 되돌리고, marker 전용 `.part`는 같은 digest 재시도에서 검증 후 다시 씁니다.
- 예상 밖 파일, symlink·hardlink·FIFO·장치, 경로 탈출과 group/world writable 경계를 거부합니다.
- `config.json`은 최대 1MiB의 일반 JSON 객체로 읽고, 선언된 양자화 방식이 marker identity와 다르면 활성화하지 않습니다.
- 중단 재시도는 매니페스트별 고정 staging 하나를 사용해 반복 실패에 따른 무한 디렉터리 누적을 막습니다.
- 디스크 계산은 검증된 완성 파일과 부분 파일의 실제 할당량만 반영하고 기본 여유 공간을 남깁니다.
- 자동 재귀 삭제, 자동 cache eviction과 공식 quarantine는 아직 없습니다. staging·비활성 final의 수동 격리도 모든 활성 참조가 없음을 확인한 정확한 digest 경로 하나로 제한합니다. 여러 매니페스트가 공유할 수 있는 CAS 청크는 전역 미참조를 증명할 수 없으면 옮기거나 삭제하지 않습니다.

패키지의 production Agent는 root로 실행하므로 기본 경로의 생성 캐시는 root 소유가 됩니다. 준비 라이브러리 자체는 설정 루트와 가장 가까운 기존 조상을 현재 유효 사용자 소유로 요구하므로 테스트·로컬 임베딩 override에서는 반드시 root만 허용하는 것은 아닙니다. 인벤토리와 벤치마크는 `/var/lib/dure/models` 루트와 후보·`config.json`·marker를 현재 Agent 사용자 소유이며 group/world writable이 아닌 항목으로 검사하지만, 상위 `/var/lib/dure`를 매번 재검사하지는 않습니다. 패키지는 그 상위를 `root:dure` `0750`, 서버 쓰기 상태만 `/var/lib/dure/server`의 `dure:dure` `0750`으로 둡니다. 이는 비-root `dure` 서버 계정의 캐시 루트 교체를 막지만 로컬 root 침해를 방어하지는 않습니다. Dure는 NVIDIA host driver를 설치하거나 변경하지 않습니다.

저널 경계가 정상일 때 로컬 attempt journal은 마지막 폐쇄형 상태만 보존하고 URL·token·응답 본문·예외 원문을 기록하지 않습니다. 루트·권한·저널 I/O 자체가 실패하면 원래 작업의 로컬 실패 상태도 남기지 못할 수 있습니다. 중앙 준비는 별도의 preparation·노드·단계·시도 상태와 폐쇄형 실패 코드를 보존하지만 자동 경보와 전역 캐시 참조 관측은 아직 없습니다.

이 계층은 SHA-256 기대값과 받은 바이트의 일치만 증명합니다. 매니페스트 작성자, 모델 게시자, 라이선스, 악성 코드 부재나 origin 운영자를 인증하지 않습니다. 중앙 준비의 `SUCCEEDED`도 정확한 등록 바이트와 OCI 다이제스트가 해당 노드에 있었음을 뜻할 뿐 게시자 신뢰나 모델 안전성 증명이 아닙니다.

## 준비 작업과 배포 소비 게이트

추천 수락 뒤의 deployment 준비는 preview와 apply를 분리합니다. preview는 불변 계획과 노드 행만 만들고, 엄격한 `apply=true` 뒤에만 전용 서비스가 `PREPARE_MODEL`을 큐잉합니다. 일반 task 생성 API는 `PREPARE_MODEL`과 `PREPARE_IMAGE`를 거부하며 payload는 preparation·배포·노드·시도 식별자와 고정 모델·런타임 식별자만 표현합니다. raw URL, credential, 임의 HTTP header, 명령, Docker 인자, 환경 변수, 마운트나 호스트 경로는 허용하지 않습니다.

- preview와 최초 적용 직전에 승인·온라인·신선한 인벤토리·보수적인 디스크, 등록 매니페스트와 OCI 이미지 다이제스트를 다시 검사합니다. 실패 재시도에서는 부분 CAS·staging의 실제 점유량을 중앙이 알 수 없으므로 최초 최악 조건 디스크 계산을 반복하지 않고, Agent가 네트워크 쓰기 전에 실제 파일시스템별 남은 바이트를 권위 있게 검사합니다. 불확실한 항목은 허용으로 추론하지 않습니다.
- 모델 단계의 전체 해시·marker·exact path 증적이 성공한 노드에만 이미지 단계를 만듭니다. 이미지는 canonical `repository@sha256:...`만 허용하고 태그와 다이제스트를 함께 넣은 모호한 참조는 중앙과 Agent의 공용 검증기에서 거부합니다.
- 이미지 단계는 정확한 digest를 inspect하고 필요할 때 같은 참조만 pull한 뒤 다시 inspect합니다. 컨테이너 run·start·stop·remove는 호출하지 않습니다.
- preparation, 노드, 단계, task ID와 증가하는 시도 번호가 현재 행과 모두 일치해야 claim·완료·실패를 반영합니다. 재시도 뒤 과거 임대의 늦은 보고는 성공을 가장하거나 새 실패를 덮어쓸 수 없습니다.
- 일부 노드 실패는 성공 증적을 보존한 `PARTIAL_FAILED`, 전체 실패는 `FAILED`로 닫힙니다. 재적용은 실패한 현재 단계만 새 시도로 만들고 성공한 모델·이미지 단계를 반복하지 않습니다.
- 추천 세대의 배포 apply는 모든 노드의 exact-path 모델과 이미지 증적이 `SUCCEEDED`여야 합니다. 기존 수동 deployment 호환 경로로 이 게이트를 우회할 수 없습니다.
- 추천 세대 롤백은 대상의 기존 성공 증적과 로컬 검증 캐시·다이제스트 이미지만 사용합니다. 새 준비 작업, 모델 네트워크 다운로드와 이미지 pull을 만들지 않습니다. 증적 뒤의 수동 삭제는 실시간 감시하지 않으므로 아티팩트가 사라졌다면 소스 중지 뒤 대상 시작이 실패할 수 있습니다.

기본 준비는 각 노드에 동일한 모델 전체 `FULL_SNAPSHOT`을 둡니다. `--stage-variant`를 명시하면 각 노드에는 서버가 그 UUID와 PP rank에 고정한 서로 다른 정규 매니페스트만 준비하고, 컨테이너에는 계산된 host path만 `/models/model:ro`로 연결합니다. task가 host path·loader 인자를 제공할 수 없고 시작 직전 캐시 전체를 다시 해시합니다. 다만 이것은 게시자·builder 서명이나 악성 root·Docker daemon 방어를 대신하지 않습니다.

## 모델 레지스트리, 승격 게이트와 추천 수락의 경계

모델 레지스트리의 영속 스키마, 관리자 인증 API, 고정 리비전·다이제스트 검증, 구조화된 벤치마크 증적, `ACTIVE` 승격 게이트, 정확한 다중 노드 증적을 소비하는 정책 기반 추천 스냅샷과 명시적 수락은 구현되었습니다. 추천은 호스트 변경 권한이 아닙니다. 저장된 인벤토리와 `ACTIVE` 모델 릴리스만 평가하고, 승인·온라인·프로필 신선도·GPU 아키텍처를 통과한 노드만 선택합니다. 수락도 적용 권한이 아니며 적용 전 배포 세대 한 건만 만듭니다.

- 아티팩트는 변경 불가능한 리비전과 매니페스트 다이제스트를 가져야 하며, 정규 매니페스트를 등록할 때 기존 다이제스트와 정확히 일치해야 합니다.
- 런타임 이미지는 정확한 OCI 다이제스트로 고정합니다.
- 라이선스와 사용 조건은 릴리스 승격 전에 검토합니다.
- 레지스트리 API는 허용 필드 외 입력을 거부하며 임의 셸, Docker 인자 목록, 호스트 경로, 마운트, 환경 변수를 저장하지 않습니다.
- 추천 생성 API는 `refresh`, 임의 명령·Docker 인자·환경 변수·마운트와 네트워크 증적 우회 입력을 거부합니다. 불변 추천·인벤토리 스냅샷만 멱등 저장하며 배포·작업·감사 이벤트를 생성하지 않습니다.
- 추천 수락 API는 정의되지 않은 실행 필드를 거부하고 저장 스냅샷과 현재 콘텐츠 ID·카탈로그·정책·인벤토리·선택 결과를 모두 재검사합니다. 성공 시 적용 전 배포 세대와 감사 이벤트만 만들고 다운로드·pull·작업·Docker 변경은 만들지 않습니다.
- 반복 수락은 같은 세대를 반환하고 감사 이벤트를 중복 생성하지 않습니다. 이전 세대는 같은 계보의 최신 세대만 허용해 의도하지 않은 분기를 차단합니다.
- 추천 인벤토리 지문과 배정에는 에이전트가 보고한 이름이 아니라 서버가 발급한 노드 UUID를 사용합니다.
- 증적은 모델 릴리스, 배치 프로필, 정렬된 중앙 노드 UUID와 현재 프로필 지문에 결합됩니다. 프로필이나 레지스트리 식별자가 달라지면 승격을 거부합니다.
- 모든 배치 프로필의 최신 증적이 통과해야 하며, 실패한 최신 결과를 과거 통과 결과로 대체하지 않습니다.
- 증적 API는 프롬프트, 자격 증명, 모델 접근 토큰, 로그, 명령, Docker 인자, 환경 변수, 마운트, 호스트 경로와 자유 형식 metadata를 받지 않습니다.
- 증적 등록과 승격은 Agent 작업, 다운로드, Docker 실행, 배포 생성이나 기존 서비스 중지를 수행하지 않습니다.
- 오래된 인벤토리, 폐기된 아티팩트, 고정되지 않은 이미지와 검증되지 않은 네트워크는 추천 또는 승격의 해당 단계를 실패 안전 방식으로 차단합니다.
- 중앙 다중 노드 추천은 정확히 정렬된 노드 UUID 조합·모델 릴리스·배치 프로필·아티팩트·런타임·현재 인벤토리 지문에 결합된 최신 `PASSED` 증적만 사용합니다. 24시간보다 오래됐거나 미래 시각인 증적, 다른 조합·식별자·지문의 증적, 배치 RTT·대역폭·손실·NCCL 기준을 통과하지 못한 결과는 거부합니다.
- 같은 노드 조합의 최신 증적이 실패했거나 통과 증적 이후 벤치마크 실행이 준비·큐·실행 중 또는 실패 상태이면 과거 결과로 우회하지 않습니다. 서로 다른 통과 증적의 노드를 섞어 새 조합을 만들지도 않습니다.
- 이 추천 게이트는 읽기 전용입니다. 추천 생성과 수락은 모델 다운로드·이미지 내려받기·Agent 작업·Docker 실행·기존 서비스 중지를 수행하지 않습니다. 로컬 `dure plan --model auto`의 기존 3×24GB 호환 예외만 계획에 사전 검증 경고를 남기고 중앙 증적 검사를 건너뜁니다.
- 현재 `NodeAssignment`로 안전하게 표현할 수 없는 tensor-parallel 배치는 추천 수락에서 실패 안전 방식으로 거부합니다.
- 자동 벤치마크 준비는 DB에 고정 문맥만 만들고, 별도의 `apply=true` 요청 뒤에만 단일 노드 작업을 만듭니다. 일반 작업 생성 API로 `BENCHMARK`를 우회 생성할 수 없습니다.
- Agent는 서버 UUID, 단일 노드, 현재 인벤토리 지문과 정확한 로컬 캐시를 다시 검사합니다. 중앙 게이트는 실행기가 고르는 가장 큰 정상 GPU의 compute capability를 폐쇄형 Ampere·Ada·Hopper·Blackwell 목록으로 해석하고 런타임의 `gpu_architectures`와 비교하며, 누락·미지·불일치를 준비·적용·증적 등록·승격에서 거부합니다. 페이로드는 임의 명령·Docker 옵션·환경 변수·마운트·호스트 경로를 표현할 수 없습니다.
- 실행기는 `/var/lib/dure/models` 아래의 펼쳐진 Dure 캐시와 `.dure-model.json`, 로컬 다이제스트 이미지만 사용하고 pull·다운로드를 하지 않습니다. hub snapshot 링크는 자동 실행에서 거부합니다. 이 metadata 결합은 아직 모델 파일 서명 검증을 대신하지 않습니다.
- 실행 직전 선택 GPU compute process를 조회하고, 실행 중 process나 상위 GPU를 확인할 수 없는 MIG process가 있으면 거부합니다. 이 조회와 Docker 시작 사이의 경주는 남으므로 벤치마크 노드에서 외부 GPU 스케줄러를 병행하지 않아야 합니다. 컨테이너는 정상 GPU 한 장만 UUID로 할당하고 네트워크 없음, 읽기 전용 루트, capability 제거, 권한 상승 금지, 비 root 사용자, `restart=no`와 정확한 벤치마크 레이블을 사용합니다. RAM은 전체·가용량 중 작은 값의 절반과 32GiB 중 작은 값으로 제한하고 8GiB 미만이면 거부하며, swap 상한은 RAM과 같고 CPU quota는 논리 CPU의 절반·최대 8코어입니다. stdout·stderr는 실행 중 합계 64KiB를 넘으면 작업을 중단합니다.
- 기존 작업 부하나 다른 Dure 벤치마크 컨테이너가 감지되면 벤치마크를 거부합니다. 재시도는 폐쇄형 payload와 대상 노드 조건을 확인한 직후, 현재 빌드·캐시·프로필·가용 자원·이미지보다 먼저 정확한 작업 컨테이너를 조정하고 그 뒤 프로필을 새로 조사합니다. Docker 시작 시각에서 고정 측정 900초와 정리 여유 300초를 넘긴 정확한 활성 컨테이너 또는 같은 UUID의 `created`·`exited`·`dead` 컨테이너만 레이블을 다시 확인한 뒤 중지·제거합니다. inspect·부재 확인·stop·remove가 불확실하면 terminal 결과를 보내지 않고 임대 재시도를 기다립니다. 배포 컨테이너와 다른 벤치마크는 중지하거나 제거하지 않습니다.
- 결과와 실패 경계는 고정 스키마·실패 코드만 허용합니다. 프롬프트, 로그, stdout·stderr와 예외 원문은 중앙 작업 결과나 감사 이벤트에 저장하지 않습니다.

현재 자동 벤치마크는 승인된 신뢰 노드의 단일 GPU 작업만 실행합니다. 요청의 Dure 커밋은 공식 Debian 패키지의 읽기 전용 빌드 metadata 또는 명시적인 개발 환경 값과 대조하고, 완료 이력 재전송에도 같은 값을 요구합니다. 외부에서 등록한 다중 노드 증적을 추천에 사용할 수는 있지만, Dure가 다중 노드 네트워크·NCCL 시험이나 GPU stage 수용 harness를 자동 실행하지는 않습니다. 전체 작업 부하 매트릭스 또는 24시간 복구 검증도 증명하지 않으며, Agent 결과의 암호학적 서명과 원본 provenance도 아직 없습니다. 관리자·Agent API와 사전 설치된 벤치마크 이미지는 계속 신뢰된 운영자 경계 안에서만 사용해야 합니다.

## 세대 작업과 롤백 보안 경계

`APPLY`와 `VERIFY` operation은 노드별 단계와 시도 번호에 task를 결합합니다. claim과 완료 시 현재 operation 단계, 노드, 작업 유형과 시도 번호가 모두 일치해야 합니다. 실패 노드 재시도 뒤 과거 임대에서 늦게 도착한 성공·실패 보고는 새 시도를 덮어쓸 수 없습니다. 같은 계보에서는 실제 적용 중인 변경 하나만 허용하므로 apply·verify·rollback이 동시에 컨테이너를 바꾸지 못합니다.

`verified_at`은 계획의 전체 배정 노드가 모두 검증에 성공하고 backend별 최소 Agent 버전을 충족한 경우에만 기록합니다. legacy는 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상, `STAGE`는 0.3.19 이상이어야 하며 엄격한 backend는 전체 노드의 정확한 `pipeline-rank-contract`와 head API 검증까지 요구합니다. 일부 노드, API 검증을 생략한 엄격한 결과, 전체 배정 집합을 충족하지 않는 Ray head 전용 검증과 구 Agent 성공은 조회 가능하지만 롤백 권한으로 승격하지 않습니다.

롤백 API는 `node_ids`, 엄격한 `apply`, `serve`만 받습니다. 클라이언트가 대상 세대, 계획, 모델 다운로드, 이미지 내려받기, 임의 명령, Docker 옵션, 환경 변수, 마운트나 호스트 경로를 지정할 수 없습니다. 서버는 계보의 최신 소스와 그 소스가 직접 가리키는 상태가 `VERIFIED`이고 `verified_at`을 보유한 직전 세대를 선택하고 다음을 다시 검사합니다.

- 소스와 대상의 전체 배정 노드·토폴로지가 정확히 같습니다.
- 요청 노드 집합이 전체 배정 노드와 정확히 같고 각 노드가 승인됨·온라인입니다.
- 모든 노드의 Agent가 legacy는 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상이며 `STAGE` 대상은 0.3.19 이상입니다.
- 소스와 대상 이미지가 OCI 다이제스트로 고정돼 있습니다.
- 추천으로 만든 대상이면 전체 노드의 exact-path 모델·이미지 준비 증적이 성공했습니다.
- 같은 계보에 다른 활성 변경이 없습니다.

롤백 준비 요청은 `PREPARED` DB 레코드만 만들고 task나 호스트 변경을 만들지 않습니다. 명시적 `apply=true` 뒤에만 `STOP_SOURCE → START_TARGET → VERIFY_TARGET`을 진행하며, `serve=true`이면 Ray head에서 `START_API → VERIFY_API`를 이어서 수행합니다. `VLLM_RAY_PP_V1`은 actor 증적 없는 복구를 새 검증 세대로 만들지 않도록 `serve=true`를 필수로 요구합니다. `START_TARGET`은 항상 `serve=false`로 전체 Ray 노드를 먼저 복구합니다. 각 단계의 모든 노드가 성공해야 다음 단계로 넘어갑니다. 롤백 task는 새 아티팩트 준비, 모델 다운로드와 이미지 pull을 항상 금지하므로 추천 대상은 과거의 성공한 exact evidence와 현재 로컬 캐시·이미지를 모두 가져야 합니다.

새 컨테이너는 `dure.deployment`, `dure.generation`, `dure.node`를 모두 기록합니다. 중지·시작·검증 시 이름만 신뢰하지 않고 실제 컨테이너의 배포 ID, 세대와 노드 ID를 다시 읽습니다. 0.3.12 이전 컨테이너에만 `dure.node`가 없을 수 있으며, 이 호환 경로는 배포 ID와 세대가 모두 정확히 일치할 때만 허용합니다. 노드 레이블이 존재하면서 다르거나 배포·세대 레이블이 누락·불일치하면 해당 컨테이너를 조작하지 않습니다.

현재 롤백은 동일 GPU에서 소스를 중지한 뒤 대상을 다시 생성합니다. 서비스 연속성을 보장하는 블루·그린 배포가 아니며 중단이 발생할 수 있습니다. 또한 저장된 네트워크·NCCL 증적을 새로 측정하거나 24시간 복구를 검증하지 않으므로, 다중 노드 운영자는 별도 수용 검사 없이 이 기능을 무중단 또는 장기 안정성 증거로 해석해서는 안 됩니다.

0.3.12 업그레이드는 controller와 migration을 먼저 적용하고 Agent를 나중에 작은 단위로 진행합니다. migration downgrade는 `active_lineage_id IS NOT NULL`인 operation, 상태가 `PREPARED`·`QUEUED`·`RUNNING`인 operation 또는 operation에 연결된 `QUEUED`·`RUNNING` task가 있으면 거부합니다. 이 검사를 우회해 task 연결이나 operation 행을 직접 삭제하면 감사와 재시도 펜싱이 깨질 수 있습니다.

## 공개 알파 전 통과 기준

신뢰된 운영자 그룹 밖의 노드를 받기 전 다음을 완료해야 합니다.

1. tokenless join의 rate limit, quota, abuse control을 추가합니다.
2. bearer-only Agent 인증을 mTLS 또는 서명된 device key로 대체합니다.
3. 사설 network overlay와 host firewall을 배포하고 검증합니다.
4. image signature, provenance, model manifest를 검증합니다.
5. root Agent와 container isolation을 독립 검토합니다.
6. join flood, heartbeat 손실, 반복 task 실패, credential 오용을 알리는 alert를 추가합니다.
