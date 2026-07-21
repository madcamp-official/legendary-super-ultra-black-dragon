# vLLM 단계 아티팩트 생성과 검증

> 상태: **생성·등록·검증, 자동 추천, 중앙 캐시 수명 주기와 rank별 노드 준비·실행 계층 구현**. Dure 0.3.20은 정확한 `VALIDATED` variant와 독립 `FULL_SNAPSHOT`을 결정론적 후보로 평가하고, 수락한 세대에 캐시·매니페스트·variant·loader·backend·rank·증적을 고정합니다. 선택된 `STAGE`의 각 pipeline rank 매니페스트는 명시적 준비 적용으로 해당 노드에 내려받아 원자적으로 활성화하고, `VLLM_RAY_PP_V1`의 vLLM 0.9.0 `sharded_state` 로더로 소비합니다.

## 이 기능이 해결하는 범위

기존 `FULL_SNAPSHOT`은 모델 전체 파일을 모든 대상 노드에 둡니다. `STAGE`는 검증된 원본 스냅샷을 vLLM이 읽을 수 있는 pipeline rank별 상태로 미리 내보내고, 각 rank에 필요한 파일 집합을 별도 정규 매니페스트로 고정합니다.

```text
검증된 FULL_SNAPSHOT 원본
        ↓ 신뢰된 오프라인 빌더
digest 고정 런타임 + vLLM 0.9.0 V0 executor
        ↓ PP worker별 네이티브 sharded-state 저장
stages/0  stages/1  ...  stages/<PP-1>
        ↓ 각 디렉터리의 정규 파일·청크 매니페스트
DRAFT variant + rank별 stage manifest
        ↓ 실제 GPU export/load 검증 증적
VALIDATED 또는 REVOKED
```

생성·등록과 배포 소비는 분리됩니다. `VALIDATED` variant가 있다는 사실만으로 다운로드나 컨테이너 변경이 일어나지 않습니다. 추천기가 선택한 exact variant를 운영자가 수락하면 그 전달 계약이 불변 세대에 저장되고, 별도의 `deployment prepare --apply`를 명시해야 rank별 준비 task가 생깁니다. 모든 노드의 모델·이미지 증적이 성공한 뒤에도 별도의 배포 적용이 필요합니다.

현재도 다음 작업은 하지 않습니다.

- 노드 간 P2P·swarm 전송이나 중앙 task가 지정한 임의 URL·경로 사용
- 운영 Agent에서 stage 변환·재분할 또는 NVIDIA host driver 변경
- 실패한 배포의 자동 failover·자동 롤백·기존 컨테이너 자동 교체
- 캐시 용량 기반 자동 퇴출·삭제 또는 격리된 파일의 자동 삭제

`STAGE`와 `FULL_SNAPSHOT`은 서로 독립된 추천 후보입니다. 같은 품질·모델에서는 실행 가능한 `STAGE`를 먼저 평가하지만, 수락 뒤 준비 실패를 다른 종류로 자동 대체하지 않습니다.

## 현재의 최소 안전 지원 범위

0.3.17은 다음 조합만 지원 대상으로 인정합니다.

| 항목 | 허용 값 |
|---|---|
| vLLM | 정확히 `0.9.0` |
| 실행기 | V0 executor |
| 모델 계열 | `qwen2.5` |
| 아키텍처 | `Qwen2ForCausalLM` |
| 양자화 | AWQ |
| tensor parallel | `TP=1` |
| pipeline parallel 생성·등록 | `1<=PP<=64`, rank가 `0..PP-1`로 완전해야 함 |
| 번들 GPU acceptance | `PP=1`만 native export/load 검증 |
| 로더 | vLLM `sharded_state`, 레지스트리 계약 `VLLM_SHARDED_STATE_V1` |
| 원격 모델 코드 | 금지, `trust_remote_code=false` |
| 빌더 런타임 | OCI SHA-256 다이제스트로 고정 |

LoRA·adapter, MoE, 멀티모달 모델, 임의 아키텍처와 사용자 모델 코드는 지원 범위가 아닙니다. `auto_map`이나 Python 모델 파일처럼 원격 코드를 요구하는 입력도 거부합니다. 지원 범위를 넓히려면 아키텍처·양자화·토폴로지별 실제 export/load 검증과 별도 코드 검토가 먼저 필요합니다.

이 제한은 현재 저장된 모델 카탈로그 전체가 자동으로 stage 변환 가능하다는 뜻이 아님을 분명히 합니다. 레지스트리의 모델 이름이 비슷해도 정확한 아키텍처, 양자화, 원본 매니페스트와 고정 런타임 계약이 모두 일치해야 합니다.

builder와 중앙 레지스트리는 PP 1~64의 구조를 표현하지만, 실제 배포 backend는 `PP=2/3`만 지원합니다. builder용 `scripts/acceptance-vllm-stage-builder.py`는 `PP=1` export·native load를 검증하고, 배포용 `scripts/acceptance-vllm-stage-ray-pp.py`는 신뢰된 2·3노드 환경에서 이미 준비한 서로 다른 rank 캐시를 실제 Ray/vLLM에 결합해 load·최소 추론을 확인합니다. 전제 조건이 없거나 opt-in하지 않은 `NOT_RUN(77)`은 성공 증적이 아닙니다.

## 현재 다중 노드 실행과의 관계

`VLLM_RAY_PP_V1`과 stage variant는 서로 다른 계약입니다.

| 구분 | `FULL_SNAPSHOT` | 0.3.20 `STAGE` |
|---|---|---|
| 모델 캐시 | 모든 노드에 같은 전체 매니페스트 | 각 노드에 자신의 pipeline rank 매니페스트 |
| 실행 토폴로지 | vLLM 0.9.0 V0 Ray, `TP=1`, `PP=2/3` | 같은 backend와 `--load-format sharded_state` |
| host 경로 | `/var/lib/dure/models/sha256-<manifest>` | `/var/lib/dure/models/stages/sha256-<복합-cache-identity>` |
| 컨테이너 경로 | 모든 노드 `/models/model` | 모든 노드 `/models/model`; host source만 rank별로 다름 |
| rank 결합 | 서버 UUID, head rank 0, worker RFC1918 IPv4 문자열 정렬 | 같은 결합에 variant·rank manifest·tensor-key·cache identity 추가 |
| 추천 디스크 게이트 | 노드마다 `max(2 × 전체 바이트 + 64 MiB, 배치 최소값)` | rank 노드마다 `2 × rank 전체 바이트 + 64 MiB` |
| 준비 실패 | 새 실행 단계 차단, 실패 단계 명시적 재시도 | 실패 rank만 재준비, `FULL_SNAPSHOT` 자동 대체 금지 |

`pipeline-rank-contract`는 고정된 vLLM 0.9.0 소스 규칙과 Ray 노드·actor topology를 대조하는 간접 증적입니다. `STAGE`에서는 ordered binding마다 매니페스트·tensor-key·복합 cache identity를 추가하고, 컨테이너 시작 직전에 rank 로컬 디렉터리의 전체 매니페스트와 `dure-stage.json`을 다시 해시합니다. 그래도 Ray가 vLLM 내부 pipeline rank를 공개 필드로 직접 보고한 것은 아니므로 실제 분산 load 증명은 opt-in GPU harness의 load·추론 결과와 함께 판단합니다.

## 추천 수락, 노드별 준비와 원자적 활성화

추천기는 source manifest, runtime release와 이미지, vLLM 버전, 아키텍처·양자화, TP·PP가 정확히 일치하는 `VALIDATED` variant를 artifact-set digest 순서의 독립 후보로 만듭니다. 네트워크 증적의 node UUID→rank 결합과 각 rank 매니페스트가 완전해야 하고, 각 노드에는 `2 × rank 전체 바이트 + 64 MiB` 이상의 여유 공간이 있어야 선택할 수 있습니다. `FULL_SNAPSHOT`은 별도 크기 조건을 가진 독립 후보이므로 잘못된 `STAGE`를 준비 중에 대체하는 fallback이 아닙니다.

수락 시 선택한 캐시 종류, source·rank 매니페스트, variant·loader 계약, 실행 backend, node UUID→rank 결합, 네트워크와 stage 검증 증적을 세대에 고정합니다. 준비 요청은 이 저장 선택을 그대로 사용합니다. `--stage-variant`를 제공하는 경우에도 저장된 digest와 같은지 확인하는 선택적 일치 단언일 뿐, `FULL_SNAPSHOT` 세대를 `STAGE`로 바꾸거나 다른 variant를 선택할 수 없습니다.

```bash
dure admin deployment prepare <deployment-id> \
  --request-id <request-uuid>

dure admin deployment prepare <deployment-id> \
  --request-id <request-uuid> --apply
```

미리보기는 task를 만들지 않고, 같은 request ID와 저장 선택에 `--apply`를 명시해야 준비를 시작합니다. 수락 뒤 variant가 `REVOKED`됐거나 검증·rank 결합이 달라졌다면 task를 만들기 전에 거부합니다. 선택한 `STAGE`가 실패해도 전체 스냅샷으로 바꾸지 않습니다.

각 `PREPARE_MODEL` payload는 공통 variant 계약과 해당 노드의 pipeline rank, rank 매니페스트, tensor-key digest만 포함합니다. raw URL·token·header·호스트 경로는 허용하지 않습니다. Agent는 root 전용 신뢰 origin에서 매니페스트 청크를 받아 다음 복합 identity를 계산합니다.

```text
원본 모델·revision·quantization
+ variant / contract / source manifest / exporter digest
+ runtime image / vLLM / architecture / loader
+ TP·PP / pipeline rank / tensor rank / tensor-key digest
        ↓ canonical SHA-256
/var/lib/dure/models/stages/sha256-<cache-identity>
```

청크·파일·stage marker를 검증한 뒤 정규 매니페스트 sidecar와 캐시 marker를 마지막에 기록하고 no-replace rename으로 활성화합니다. 기존 경로가 정확히 같으면 전체 재해시 뒤 재사용하고, 내용이 다르면 덮어쓰거나 자동 삭제하지 않고 실패합니다. 현재 모델 준비 성공과 중앙 캐시 `READY` 이벤트는 같은 트랜잭션에서 기록됩니다. 이미 더 새 준비 시도가 현재 시도가 됐다면 늦게 완료된 과거 시도는 성공 결과가 있어도 `READY`를 만들지 못합니다. 컨테이너에는 이 계산된 단일 host 경로만 `/models/model:ro`로 mount합니다.

일반 probe와 heartbeat는 대용량 가중치를 매번 전체 재해시하지 않습니다. `complete=false`인 부분 관찰과 legacy profile은 중앙 캐시 상태를 전혀 바꾸지 않습니다. 완전한 폐쇄형 probe만 중앙에 이미 알려진 identity를 조정하며, 누락은 `MISSING`, 위험·손상은 `CORRUPT`, identity 불일치는 `STALE`로 강등합니다. 완전한 probe에서 정상 항목을 관찰해도 `READY`로 승격하거나 더 나쁜 상태를 복구하지 않으며, 중앙에 없던 캐시를 새로 신뢰하지 않습니다. 준비 완료 시점과 Docker 시작 직전의 `validate_materialized_stage_cache`가 정규 sidecar, 전체 파일 digest, stage marker와 v3 캐시 marker를 다시 확인하는 권위 검사입니다.

## 중앙 캐시 수명 주기와 수동 격리

중앙 `node_artifact_caches`는 노드 UUID와 물리 캐시 identity별 현재 상태를 저장하고, `artifact_cache_events`는 준비·probe·variant 철회·실행 검증·격리에서 발생한 전이를 순번이 있는 추가 전용 기록으로 남깁니다. 같은 source ID를 다른 증적으로 재사용하는 요청은 멱등 재전송으로 보지 않고 충돌로 거부합니다.

| 상태 | 진입 원인 | 복구 방법 |
|---|---|---|
| `READY` | 현재 `PREPARE_MODEL` 시도가 전체 identity·파일·바이트를 검증해 성공 | 실행 가능 상태입니다. |
| `STALE` | stage variant 철회, probe identity 불일치, `READY` 캐시의 격리 요청 | 신뢰 원인을 해결하고 새 현재 준비를 성공시킵니다. |
| `MISSING` | 완전한 probe에서 알려진 캐시가 보이지 않음 | 캐시를 새로 준비합니다. |
| `CORRUPT` | 완전한 probe의 위험·손상 관찰 또는 배포 검증 실패 | 원인을 조사하고 필요하면 격리한 뒤 새로 준비합니다. |
| `QUARANTINED` | 수동 격리 작업이 원자적 보존 이동에 성공 | 격리본은 자동 복원·삭제하지 않으며 새 캐시를 준비합니다. |

일반 적용·시작·재시작·검증과 롤백 대상 시작은 세대가 고정한 exact identity가 `READY`이고, 그 상태를 만든 준비 시도가 여전히 현재 성공 시도이며, 최신 다이제스트 이미지 준비도 성공했을 때만 진행합니다. `STAGE`는 variant가 계속 `VALIDATED`인지도 같은 게이트에서 다시 확인합니다. 실행 검증이 캐시 문제로 실패하면 exact 캐시는 `CORRUPT`로 닫혀 다음 실행을 막습니다.

운영자는 다음 읽기 전용 명령으로 중앙 상태와 참조를 먼저 확인할 수 있습니다.

```bash
dure admin artifact-cache list
dure admin artifact-cache show <cache-id>
dure admin artifact-cache verify <cache-id>
dure admin artifact-cache quarantine <cache-id>
```

마지막 명령도 기본값은 미리보기라 task가 0개입니다. 실제 격리는 `dure admin artifact-cache quarantine <cache-id> --apply`처럼 명시해야 하며, 중앙은 준비·배포·벤치마크의 대기·실행 작업, 활성 operation, 현재 세대, 검증된 직접 롤백 선행 세대와 다른 보수적 참조가 하나라도 있거나 참조 완전성을 증명할 수 없으면 거부합니다. Agent는 Docker에서 해당 경로를 mount한 Dure 컨테이너가 없는지도 확인합니다.

허용된 격리는 정확한 캐시 디렉터리를 `/var/lib/dure/models/.dure-quarantine/<task-id>-<kind>-sha256-<identity>`로 덮어쓰기 없이 원자적 이동하고 보존합니다. 파일이나 공유 CAS 청크를 삭제하지 않으며, 용량 기준 자동 퇴출·자동 정리와 노드 간 P2P 복제도 수행하지 않습니다.

## 실패 대응 경계

1. 중앙 사전 검사 실패는 준비 task와 컨테이너 변경을 만들지 않습니다.
2. 노드별 준비 실패는 성공한 다른 rank와 모든 시도 증적을 보존하고 다음 배포 단계를 차단합니다. 현재 성공만 `READY`를 만들고 늦은 과거 완료는 상태를 되살리지 못합니다.
3. 원인을 복구한 운영자가 같은 request ID·variant digest에 `--apply`를 다시 명시하면 현재 실패 단계만 증가한 시도 번호로 재시도합니다.
4. 시작 직전 재해시나 label·readiness 불일치는 Docker 실행 전에 실패합니다. 이미 전환이 시작된 뒤 실패했다면 실제 operation·컨테이너 상태를 확인합니다.
5. 복구가 불가능하면 검증된 직전 세대로 명시적 롤백합니다. 롤백은 새 다운로드·image pull을 하지 않으며 자동 롤백이 아닙니다. `STOP_SOURCE` 성공 뒤에도 대상 exact `READY` 캐시와 최신 이미지 증적을 다시 검사하고, 실패하면 `START_TARGET` 작업을 만들지 않습니다.

긴급 `STOP`은 variant가 사후 `REVOKED`되거나 캐시가 손상돼도 정확한 Dure 배포 label로 실행할 수 있습니다. 반대로 `APPLY`·`START`·`RESTART`·`VERIFY`와 롤백 대상 시작은 중앙 exact `READY`, 현재 준비 성공과 최신 이미지 증적을 다시 요구합니다. Agent가 임의 모델로 대체하거나 NVIDIA driver를 자동 설치·교체하지 않습니다.

## `stages/<pp-rank>` 격리가 필요한 이유

vLLM 0.9.0의 `ShardedStateLoader` 기본 파일명은 `model-rank-<rank>-part-<part>.safetensors`이고, 이 `rank`는 pipeline rank가 아니라 tensor-parallel rank입니다. 현재 지원 조합은 `TP=1`이므로 모든 pipeline worker의 tensor rank가 0입니다.

여러 PP worker가 하나의 출력 디렉터리에 저장하면 모두 `model-rank-0-*` 이름을 사용해 충돌하거나 먼저 쓴 파일을 덮어쓸 수 있습니다. Dure 빌더는 각 worker가 자신의 pipeline rank를 확인하고 다음처럼 분리된 경로에 저장하게 합니다.

```text
stages/0/model-rank-0-part-0.safetensors
stages/1/model-rank-0-part-0.safetensors
...
stages/<PP-1>/model-rank-0-part-0.safetensors
```

Dure의 기존 `layer_start`·`layer_end` 값을 이용해 원본 safetensors 파일을 임의 바이트 또는 레이어 범위로 자르지 않습니다. pipeline worker 안에서 vLLM의 네이티브 sharded-state 저장기를 호출하고 PP 디렉터리만 격리합니다. 이 계약은 [vLLM 0.9.0 sharded-state 예제](https://docs.vllm.ai/en/v0.9.0/examples/offline_inference/save_sharded_state.html)와 [ShardedStateLoader API](https://docs.vllm.ai/en/v0.9.0/api/vllm/model_executor/model_loader/sharded_state_loader.html)에 고정합니다.

## 신뢰된 오프라인 빌더 계약

빌더는 커뮤니티 GPU Agent의 원격 작업이 아닙니다. 신뢰된 운영자가 네트워크와 쓰기 범위를 통제한 별도 환경에서 실행합니다.

1. 로컬 `FULL_SNAPSHOT`과 그 정규 매니페스트 파일을 함께 읽고, 계약의 매니페스트 다이제스트와 대조합니다.
2. 빌더로 사용하는 런타임 이미지를 정확한 `repository@sha256:...`로 고정합니다.
3. 컨테이너 안의 실제 vLLM 버전과 지원 계약을 다시 확인합니다.
4. 각 PP worker가 자신의 `stages/<pp-rank>`에 네이티브 sharded state를 기록합니다.
5. 허용한 모델·토크나이저 metadata만 stage별로 명시적으로 복사합니다.
6. tensor key 집합, 파일 종류와 rank 완전성을 검사합니다.
7. 각 stage를 기존 정규 파일·청크 형식으로 매니페스트화하고, 완성된 결과만 게시합니다.

원본 가중치, Hugging Face index, Python 코드, adapter 파일과 예상하지 못한 특수 파일을 stage 출력에 섞지 않습니다. 심볼릭 링크·하드 링크·장치·소켓·FIFO, 절대 경로와 상위 경로 탈출도 허용하지 않습니다. 빌드 중간 디렉터리는 유효한 최종 variant가 아니며 기존 결과를 덮어쓰지 않습니다.

vLLM, PyTorch, safetensors와 CUDA 계열 의존성은 크고 호스트·드라이버 조합에 민감합니다. 기본 Debian `dure` 패키지에는 이 heavy dependency를 넣지 않습니다. 운영 Agent와 중앙 서버를 빌더 환경으로 사용하지 말고, 별도의 digest 고정 OCI 빌더에서 필요한 의존성을 함께 고정합니다. Docker가 host NVIDIA driver 자체를 호환 가능하게 만들지는 않으므로 실제 GPU 검증 환경의 driver·CUDA 호환성도 별도로 확인해야 합니다.

현재 builder 진입점은 digest 고정 OCI 환경 안에서 다음과 같이 사용합니다. 두 환경 변수는 실행 환경의 불변 identity이며 task payload나 원격 사용자 입력으로 받지 않습니다.

```bash
export DURE_STAGE_RUNTIME_IMAGE='registry.example/dure-stage-builder@sha256:<64-hex>'
export DURE_STAGE_EXPORTER_BUILD_DIGEST='sha256:<64-hex>'

python3 -m dure.stage_artifact build \
  --source /trusted/input/full-snapshot \
  --source-manifest /trusted/input/full-snapshot.manifest.json \
  --output /trusted/output/artifact-set \
  --source-manifest-digest 'sha256:<64-hex>' \
  --pipeline-parallel-size 3

python3 -m dure.stage_artifact verify \
  --artifact-set /trusted/output/artifact-set \
  --index-digest 'sha256:<64-hex>'
```

`--source-manifest-digest`는 variant 계약이 가리키는 불변 identity이고, `--source-manifest`는 빌드 시점에 실제로 읽을 정규 JSON 매니페스트입니다. 둘을 함께 요구하는 이유는 과거에 확인한 다이제스트만 신뢰하지 않고 빌드 직전에 매니페스트의 정규 다이제스트, `FULL_SNAPSHOT` marker, 전체 파일 경로·크기·SHA-256과 예상하지 못한 추가 파일 여부를 다시 검사하기 위해서입니다. 매니페스트와 디렉터리의 현재 내용이 정확히 일치하지 않거나 검사 중 파일 identity가 바뀌면 export 전에 실패합니다.

`build`는 기존 output을 교체하지 않습니다. `verify`는 파일·rank·tensor·매니페스트와 index digest를 다시 검사하지만 실제 vLLM GPU load 검증은 아닙니다. 이 로컬 검사를 `GPU_EXPORT_LOAD/PASSED` 증적으로 바꾸어 등록하면 안 됩니다.

번들 GPU 수용 검사는 `scripts/acceptance-vllm-stage-builder.py`입니다. 기본 실행과 opt-in·load 확인이 없는 실행, GPU·고정 환경 전제 조건이 부족한 실행, `PP>1` 요청은 구조화된 `NOT_RUN`과 종료 코드 77을 반환해야 합니다. `DURE_RUN_STAGE_GPU_ACCEPTANCE=1`과 `DURE_STAGE_ACCEPTANCE_LOAD=1`을 모두 명시하고 다음처럼 local source와 정규 매니페스트 파일·digest·output을 제공한 `PP=1` 환경에서만 실제 검증을 시작합니다.

```bash
export DURE_RUN_STAGE_GPU_ACCEPTANCE=1
export DURE_STAGE_ACCEPTANCE_LOAD=1
export DURE_STAGE_ACCEPTANCE_SOURCE=/trusted/input/full-snapshot
export DURE_STAGE_ACCEPTANCE_SOURCE_MANIFEST=/trusted/input/full-snapshot.manifest.json
export DURE_STAGE_ACCEPTANCE_SOURCE_MANIFEST_DIGEST='sha256:<64-hex>'
export DURE_STAGE_ACCEPTANCE_OUTPUT=/trusted/output/acceptance-artifact-set
export DURE_STAGE_ACCEPTANCE_PP=1
export DURE_STAGE_RUNTIME_IMAGE='registry.example/dure-stage-builder@sha256:<64-hex>'
export DURE_STAGE_EXPORTER_BUILD_DIGEST='sha256:<64-hex>'

python3 scripts/acceptance-vllm-stage-builder.py
```

GPU harness도 동일한 전체 source 재검증을 export 직전에 수행합니다. 시작 뒤 export·native load·최소 추론이 모두 성공해야 `PASSED`이며, 시작 뒤 실패는 `FAILED`입니다. 상위 자동 테스트가 `NOT_RUN`을 성공으로 바꾸거나 종료 코드 0으로 정규화해서는 안 됩니다.

## variant 동일성과 rank 매니페스트

variant는 모델 이름만으로 식별하지 않습니다. 결정론적 동일성에는 최소한 다음 값이 결합됩니다.

- 원본 `FULL_SNAPSHOT` 매니페스트 다이제스트
- 런타임 OCI 이미지 다이제스트와 vLLM 버전
- exporter 빌드 다이제스트
- 아키텍처와 양자화
- TP와 PP 크기
- loader 형식
- rank 순서로 정렬한 각 stage 매니페스트와 tensor 요약

입력 배열의 제출 순서가 달라도 rank로 정렬한 논리 내용이 같으면 같은 identity가 됩니다. 반대로 같은 원본·런타임·토폴로지에서 stage 매니페스트가 달라지면 정상적인 두 번째 결과로 자동 인정하지 않고 비결정성, 잘못된 빌드 또는 변조 가능성이 있는 충돌로 취급합니다.

등록은 모든 PP rank를 한 번에 다룹니다. 중복 rank, 음수 rank, `rank>=PP`, 빠진 rank, 다른 TP·PP나 source/runtime에 속한 매니페스트를 섞은 입력은 거부합니다. 각 stage 매니페스트는 기존 아티팩트 매니페스트와 같은 정규 경로·파일·청크 계약을 사용합니다.

## 중앙 관리자 API

현재 중앙 variant 레지스트리는 관리자 인증 API로 제공합니다. 전용 `dure admin` 하위 명령은 아직 없으며, 오프라인 파일 생성·검사는 앞 절의 `python3 -m dure.stage_artifact` 진입점을 사용합니다.

```text
POST /v1/admin/stage-artifact-variants
GET  /v1/admin/stage-artifact-variants
GET  /v1/admin/stage-artifact-variants/{artifact_set_digest}
POST /v1/admin/stage-artifact-variants/{artifact_set_digest}/evidence
POST /v1/admin/stage-artifact-variants/{artifact_set_digest}/transition
```

등록 본문은 source manifest, canonical digest-pinned runtime image, vLLM·exporter·아키텍처·양자화·TP·PP·loader와 전체 stage 목록만 받는 폐쇄형 스키마입니다. source manifest는 기존 모델 아티팩트에 결합돼 있어야 하고 runtime image도 같은 vLLM 버전의 등록된 runtime release여야 합니다. 각 stage는 pipeline/tensor rank, manifest와 그 digest, tensor key 개수·digest와 safetensors weight 크기를 함께 제출합니다. 중앙은 stage manifest를 다시 정규화하고 다이제스트·rank·크기를 검증한 뒤에만 원자적으로 등록합니다.

증적 본문도 schema version, variant identity, canonical UUIDv4 `validation_run_id`, `SYNTHETIC` 또는 `GPU_EXPORT_LOAD`, `PASSED`·`FAILED`·`NOT_RUN`, validator version·build digest, 폐쇄형 failure code와 rank 결과만 받습니다. 같은 run ID와 같은 내용의 재전송만 멱등하며, 같은 run ID에 다른 내용을 연결하면 충돌합니다. `DRAFT`에서 수행하는 새 재검증은 새 run ID와 증가한 등록 순번을 사용합니다. 이미 등록한 run의 정확한 재전송은 이후 상태와 무관하게 기존 결과를 반환하지만, `VALIDATED`와 `REVOKED`에는 새 run을 등록할 수 없습니다. 전이 본문은 목표 상태 하나만 받습니다. 알 수 없는 필드, raw 로그, URL, credential, 명령과 호스트 경로는 허용하지 않습니다.

## 검증 상태와 증적

상태 전이는 다음과 같습니다.

```text
DRAFT ── 최신 GPU_EXPORT_LOAD / PASSED ──> VALIDATED
   └──────────────────────────────────────> REVOKED
VALIDATED ────────────────────────────────> REVOKED
```

- 등록이 완료된 variant는 `DRAFT`입니다. 등록 성공은 실제 GPU에서 load할 수 있다는 증명이 아닙니다.
- synthetic 검사는 tensor coverage, 누락 rank, 잘못된 파일, 다이제스트 변조와 identity 결정성을 빠르게 검사하지만 승격 증적이 아닙니다.
- 정확한 variant identity에 결합된 실제 GPU `GPU_EXPORT_LOAD` 검증의 최신 결과가 `PASSED`일 때만 `VALIDATED`로 전환할 수 있습니다.
- GPU·모델 fixture·고정 이미지 같은 전제 조건이 없어 검증하지 못한 경우 결과는 `NOT_RUN`입니다. `NOT_RUN`은 성공이나 skip-pass가 아니며 승격할 수 없습니다.
- `DRAFT` 승격에서는 최신 GPU 증적이 `FAILED` 또는 `NOT_RUN`이면 과거 `PASSED`로 우회하지 않습니다.
- 새 validation run 증적은 `DRAFT`에서만 추가합니다. 이미 등록한 동일 run의 정확한 네트워크 재전송은 상태 전환 뒤에도 기존 증적을 반환하지만, `VALIDATED`와 `REVOKED`에서 새 run을 추가하는 요청은 거부합니다. 검증 뒤 신뢰 문제가 발견되면 운영자가 영향 범위를 검토해 명시적으로 `REVOKED`로 전환하고, 수정된 계약은 새 `DRAFT` variant에서 검증합니다.
- 각 실제 검증 실행은 새 canonical UUIDv4 `validation_run_id`를 사용합니다. 동일 run의 네트워크 재전송만 기존 증적을 반환하고, 새 실행은 결과 수치가 같아도 새 순번의 증적입니다.
- 잘못 발행됐거나 더 이상 신뢰하지 않는 variant는 `REVOKED`로 전환합니다. `REVOKED`를 다시 활성 상태로 되돌리지 않습니다.

증적에는 프롬프트, 모델 토큰, 원본 로그, stdout·stderr, 임의 명령, Docker 인자, 환경 변수, 마운트나 호스트 경로를 넣지 않습니다. 필요한 큰 로그는 접근 제어된 별도 저장소에 보관하고 중앙에는 폐쇄형 결과와 안전한 수치·다이제스트만 남깁니다.

## 실패 대응

| 실패 | 기본 처리 | 운영자 대응 |
|---|---|---|
| 지원하지 않는 아키텍처·양자화·vLLM·TP | 빌드 시작 전 거부 | 계약을 우회하지 말고 별도 지원 검증을 추가합니다. |
| remote code·LoRA·MoE·멀티모달 감지 | 입력 거부 | 신뢰 경계를 확장하지 말고 승인된 원본을 다시 준비합니다. |
| PP rank 누락·중복 또는 예상 밖 파일 | variant 등록·승격 거부 | partial 결과를 조립하지 말고 동일한 고정 입력으로 전체 빌드를 새로 검증합니다. |
| 파일·청크·tensor digest 불일치 | 변조 가능성이 있는 실패로 보존 | 원본, 빌더 이미지와 출력 저장소를 조사하며 해시를 다시 계산해 우회하지 않습니다. |
| 빌더 중단 또는 디스크 부족 | 중간 결과를 최종 게시하지 않음 | 원인을 해결하고 격리된 staging에서 다시 실행합니다. 기존 variant를 덮어쓰지 않습니다. |
| GPU 검증 전제 조건 부족 | `NOT_RUN` | 적합한 GPU·driver·digest 고정 이미지를 준비해 실제 검증을 새로 실행합니다. |
| 실제 export/load 실패 | `DRAFT`는 비승격, `VALIDATED`에는 새 실패 run 등록 거부 | driver/CUDA/runtime/모델 계약과 영향 범위를 조사합니다. 검증된 variant의 신뢰가 깨졌다면 명시적으로 `REVOKED`로 전환하고 수정된 계약은 새 `DRAFT` variant에서 검증합니다. |
| 운영 중 신뢰 철회 | `REVOKED`와 연결된 중앙 캐시는 `STALE` | 후속 선택·실행을 차단하고 영향 범위를 조사합니다. 기존 컨테이너 정지는 별도 운영 판단과 명시적 배포 작업으로 수행합니다. |
| 완전한 probe에서 누락·손상·identity 불일치 | 중앙 캐시를 `MISSING`·`CORRUPT`·`STALE`로 강등 | 원인을 확인하고 수동 격리 또는 새 준비를 수행합니다. probe만으로 `READY`를 복구하지 않습니다. |
| 실행 검증에서 exact 캐시 실패 | 중앙 캐시를 `CORRUPT`로 기록하고 다음 실행 차단 | 컨테이너와 캐시 증적을 조사하고 필요하면 격리한 뒤 새 준비를 성공시킵니다. |
| 격리 참조 또는 활성 mount 존재 | task 생성 또는 Agent 이동 거부 | 작업·operation·현재/롤백 세대 참조를 안전하게 종료한 뒤 다시 미리보기하고 명시적으로 적용합니다. |

빌드·등록·검증 실패는 추천, 준비 operation, Agent task, Docker 컨테이너를 자동으로 만들거나 변경하지 않습니다. 실행 중인 이전 배포도 자동 중지하거나 stage variant로 교체하지 않습니다. 이 실패 격리는 “새 variant를 사용할 수 없음”을 뜻하며 “기존 배포가 자동 복구됐다”는 뜻은 아닙니다. 기존 배포의 상태는 별도 health check와 세대별 검증으로 계속 확인합니다.

## 후속 범위

0.3.20은 node UUID와 PP rank를 매니페스트에 고정하고, exact `VALIDATED` variant와 독립 `FULL_SNAPSHOT`을 결정론적으로 선택하며, Agent가 자신의 stage만 원자적으로 준비하도록 합니다. 준비·실행·readiness 증적은 variant·rank·cache identity·OCI digest에 결합되고, 중앙 캐시 수명 주기와 수동 보존 격리가 실행 게이트에 연결됩니다.

노드 간 P2P 전송, 용량 기반 자동 퇴출·삭제, 다른 TP 값, 다른 모델 아키텍처·양자화, 다중 노드 자동 GPU 검증과 게시자·이미지 서명은 후속 검증 범위입니다. Dure가 NVIDIA host driver를 자동 설치·교체하는 기능은 후속 범위에도 포함하지 않습니다.
