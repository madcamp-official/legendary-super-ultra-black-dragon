# 단일 GPU activation 및 3×24GiB 다중 노드 릴리스 검증

이 문서는 일반 단위 테스트가 대신할 수 없는 두 운영 검증의 **절차(runbook)** 를 재현 가능하게
남깁니다. 실제 실행 결과는 이 문서에 덮어쓰지 않고 [릴리스 증적 기록](release-evidence/README.md)에
version별로 남깁니다.

- v0.4.14에서 보완한 단일 GPU activation의 staged operation 완료 대기 회귀
- `VLLM_RAY_PP_V1`에서 24GiB 이상 GPU 세 장으로 실행하는 `TP=1`, `PP=3` 수용 검사

이 절차는 실제 GPU·Docker·Ray·vLLM과 중앙 제어면을 사용합니다. CI나 개발용 fake runner의
통과를 실제 수용 검사 통과로 기록해서는 안 됩니다. 운영 중인 workload를 대상으로 failure
경로를 만들지 말고, 승인된 격리 검증 노드와 새 deployment generation을 사용합니다.

## 공통 증적

실행 시작 전에 다음 값을 하나의 운영 기록에 보관합니다. credential, enrollment token,
`/etc/dure/agent.json`·`/etc/dure/server.env`의 내용, raw model prompt나 private URL은 기록하지
않습니다.

- Dure Debian package version과 `/usr/share/dure/build-commit`
- source tag·commit, model manifest digest, OCI runtime image digest
- 중앙 deployment ID·generation·validation run ID
- 노드 UUID, 선택 GPU index·UUID, 사설 runtime address
- controller operation·task ID와 최종 structured result

`NOT_RUN`과 종료 코드 `77`은 전제 조건 미충족입니다. 성공 증적이 아니며 `PASSED`로 바꾸거나
다른 실행의 성공을 재사용해서는 안 됩니다.

## v0.4.14에서 도입한 단일 GPU activation 순서 회귀

v0.4.14는 `APPLY_DEPLOYMENT` task의 성공만으로 최종 VERIFY를 보내지 않도록 변경했습니다.
activation은 최신 `APPLY` operation이 terminal `SUCCEEDED`가 될 때까지 기다리고, `FAILED` 또는
`PARTIAL_FAILED`면 최종 VERIFY를 만들지 않아야 합니다.

### 단일 GPU 사전 조건

- 재검증 대상 package와 server·Agent build commit을 고정합니다. activation spec의
  `benchmark.dure_commit`은 대상 package의 `/usr/share/dure/build-commit`과 같아야 합니다.
- 대상은 승인·온라인 상태의 단일 GPU 노드이며, placement는 `single-gpu`, `PP=1`, `TP=1`입니다.
- 모델 revision·manifest와 runtime image는 불변 digest로 고정하고, benchmark가 요구하는 GPU,
  disk, Docker·NVIDIA runtime 조건을 충족합니다.
- 72B `PP=3` pod나 다중 노드를 `dure admin activate`에 넣지 않습니다. 이 명령의 자동화 범위는
  단일 GPU뿐입니다.

### 성공 경로

먼저 읽기 전용 미리보기로 대상과 변경 범위를 확인합니다.

```bash
dure admin activate --file activation.json --nodes <single-node-uuid>
```

격리 노드에서만 실제 적용합니다.

```bash
dure admin activate --file activation.json \
  --nodes <single-node-uuid> --apply
```

다음 순서가 operation·task 기록에서 지켜져야 합니다.

```text
PROBE → immutable registry registration → PREPARE/BENCHMARK → ACTIVE
→ recommendation accept → deployment preparation → APPLY_DEPLOYMENT task
→ APPLY operation SUCCEEDED → final VERIFY → READY and verified_at
```

특히 final `VERIFY` task의 생성 시각은 최신 `APPLY` operation의 terminal `SUCCEEDED` 뒤여야
합니다. API 최소 요청, GPU·container 검증, `status=READY`, `verified_at`을 함께 확인합니다.

### 실패 경로

격리 환경에서 staged `APPLY` operation을 `FAILED` 또는 `PARTIAL_FAILED`로 끝내는 failure test를
별도로 수행합니다. 다음이 모두 참이어야 합니다.

- final `VERIFY` task가 생성되지 않음
- `verified_at`이 기록되지 않음
- operation의 failure code가 보존됨
- 수정 후 같은 불변 입력과 증가한 `benchmark.attempt`로 다시 실행함

## 3×24GiB `PP=3` 다중 노드 수용 검사

이 검사는 Qwen2.5-72B AWQ 같은 72B pipeline 기준선을 위한 별도 운영 경로입니다. `TP=1`,
`PP=3`, 노드별 GPU 한 장, vLLM 0.9.0 V0 Ray 계약만 허용합니다.

### 다중 노드 사전 조건

- 승인·온라인 상태의 서로 다른 세 노드와, 각 노드에서 계획이 선택한 정상 GPU 한 장
- GPU마다 실제 total memory 24000MiB 이상
- 서로 다른 canonical RFC1918 IPv4, 같은 기본 network interface, 정확한 UUID→rank binding
- digest 고정 runtime image, 정확한 `FULL_SNAPSHOT` 또는 rank별 `STAGE` cache의 `READY` 증적
- Ray GCS `6379`와 worker 범위 `20000-21000`은 세 노드 사이에서만 허용하고 공용 인터넷에는 노출하지 않음

중앙 추천의 network/NCCL 증적을 우회하는 로컬 계획 호환 예외는 수용 검사의 근거가 될 수 없습니다.

### GPU harness

root 소유이며 group/world writable이 아닌
`/etc/dure/acceptance-vllm-ray-pp-v1.json`에 계획과 같은 `ordered_bindings`를 기록합니다. 3×24GiB
프로필은 다음처럼 `minimum_gpu_memory_mib`를 추가합니다. 이 값은 임의 조정할 수 없으며 정확히
`24000`이고 binding은 정확히 세 개여야 합니다.

```json
{
  "minimum_gpu_memory_mib": 24000,
  "ordered_bindings": ["<rank-0 binding>", "<rank-1 binding>", "<rank-2 binding>"]
}
```

실제 설정에는 기존의 모든 폐쇄형 필드도 함께 있어야 하며, 위 배열 표기는 축약 예시입니다. harness는
Ray custom resource로 각 binding을 고정한 뒤, model load 전에 각 노드에 GPU 하나씩을 예약해 실제
CUDA total memory를 측정합니다. 24000MiB 미만, 누락 또는 형식 오류는 `GPU_MEMORY_INSUFFICIENT` 또는
`GPU_MEMORY_ATTESTATION_*` 실패입니다.

`FULL_SNAPSHOT` 검사는 신뢰된 digest-pinned wrapper 안에서 실행합니다.

```bash
DURE_RUN_VLLM_RAY_PP_ACCEPTANCE=1 \
  python3 scripts/acceptance-vllm-ray-pp.py
```

`STAGE` cache 경로는 별도 `scripts/acceptance-vllm-stage-ray-pp.py`를 사용합니다. wrapper는 실제
OCI image digest, 중앙 계획, 설정 파일을 대조한 기록을 남겨야 합니다. harness의
`runtime_image_declared`는 선언값이고 그 자체로 image provenance를 증명하지 않습니다.

`PASSED` 결과에는 세 GPU의 측정값 `gpu_memory_mib`, 최소값, `pipeline-rank-contract`, 최소 추론
결과가 있어야 합니다. 실제 Ray 연결·distributed load 시작 뒤의 오류는 `FAILED`이며 `NOT_RUN`으로
낮추지 않습니다.

### 중앙 deployment 검증

harness 성공은 controller의 준비·apply·verify를 대체하지 않습니다. 전체 노드를 명시해 별도로
진행합니다.

```bash
dure admin deployment prepare <deployment-id> --request-id <request-uuid> --apply
dure admin apply <deployment-id> \
  --nodes <node-a> <node-b> <node-c> --serve
dure admin verify <deployment-id> \
  --nodes <node-a> <node-b> <node-c> --api
```

API는 rank 0 head에서만 검사해도 `verify --api`의 node 목록에는 세 노드를 모두 넣습니다. worker는
각자의 Ray·GPU·pipeline-rank-contract를 검증해야 하며, head만 넣은 검증은 전체 세대의
`verified_at` 증적이 아닙니다.

## 합격·중단 기준

다음 중 하나라도 맞지 않으면 release 또는 새 generation을 통과시키지 않고 원인을 조사합니다.

- 단일 GPU: APPLY operation 완료 전 VERIFY 생성, operation 실패 뒤 VERIFY 생성, `READY`와
  `verified_at` 불일치
- 다중 노드: 3×24GiB 미충족, UUID·GPU·주소·rank 불일치, cache 또는 image digest drift,
  Ray node·actor topology 불일치, 최소 추론/API 실패
- 어느 경로든: `NOT_RUN`, 외부 공개 Ray port, non-digest runtime, 모호한 GPU 선택, credential이나
  임의 Docker 인자를 통한 우회

실패를 해결한 뒤에는 과거 성공 결과를 재사용하지 않습니다. 새 validation run과 현재 준비 증적으로
처음부터 확인하며, Dure는 driver를 자동 변경하거나 실패 노드를 자동 재배정·자동 롤백하지 않습니다.

실제 결과 기록에는 최소한 source commit, package version, OCI image digest, model manifest digest,
선택 GPU UUID, 실행 시각, `PASSED`·`FAILED`·`NOT_RUN` 상태와 비밀값을 제외한 구조화된 결과를
남깁니다. 템플릿은 [release-evidence/template.md](release-evidence/template.md)를 사용합니다.
