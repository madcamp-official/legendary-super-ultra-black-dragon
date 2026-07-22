# Dure 관리자 운영 검증 보고서

기준 시각: 2026-07-22 UTC

검증 방식: 중앙 Controller의 관리자 조회와 이미 저장된 실행 증적 재검토

종합 판정: **조건부 통과**

## 요약

Dure의 핵심 제어면은 실제 운영 기록으로 다음을 입증했다.

- 승인된 세 GPU 노드가 중앙에 등록되어 heartbeat를 보내고, Docker·NVIDIA runtime과
  24 GiB Ampere GPU 인벤토리를 보고했다.
- Qwen2.5 72B AWQ 모델은 revision, manifest digest, OCI runtime digest가 고정된 릴리스로
  등록되었다.
- 세 노드의 GPU를 정확한 UUID와 pipeline rank에 결합한 `TP=1`, `PP=3` 프로필이 실제
  model load, 추론, context, 재시작, 네트워크 검증을 포함한 8단계 qualification을 통과했다.
- 측정된 성능·안정성 값이 등록된 모든 승격 기준을 만족했고, 해당 릴리스와 placement가
  중앙 레지스트리에서 `ACTIVE`로 승격되었다.
- 별도의 0.5B 배포에서는 Controller가 Agent 작업을 통해 apply, API 시작, API 검증을 모두
  완료하고 배포를 `VERIFIED`로 기록했다. 따라서 중앙 작업 발행부터 Agent 실행과 결과 수집까지의
  기본 폐루프도 실제로 동작했다.
- qualification에서 기동한 72B runtime을 이용한 인증형 외부 gateway에서도 무자격 요청 거부와
  정상 모델 조회·chat 응답을 확인했다.

다만 현재 실행 중인 72B 서비스는 qualification runtime을 보존해 gateway에 연결한 경로다.
72B를 Controller의 Fleet 세대로 `accept → prepare → apply → verify`한 기록은 아직 없다. 따라서
“실제 3노드 모델이 정상 작동한다”와 “그 모델의 Fleet-native 배포 전 과정이 끝났다”를 구분하며,
후자는 이 보고서에서 `NOT_RUN`으로 판정한다.

## 검증 범위와 출처

이번 작성 과정에서는 새 workload, benchmark, qualification 또는 배포 작업을 만들지 않았다.
다음 관리자 읽기 전용 자료를 사용했다.

- node 목록과 node별 최신 profile
- model release, runtime, placement profile 레지스트리
- stage variant와 qualification evidence
- task 이력과 deployment operation 이력
- artifact cache 중앙 투영
- 기존 gateway 수용 확인 기록

운영 중인 Agent package는 `0.4.22`다. 72B qualification evidence가 기록한 Dure source commit은
`9e4b10170ebf2ae379bf3b409935ff80625a07b3`이다. 현재 개발 브랜치는 `0.4.23`이며, 이 브랜치의
변경 사항은 별도의 코드 검증을 통과했지만 아직 운영 Agent/Controller에 배포된 증적으로 취급하지
않는다.

## 판정표

| 검증 항목 | 판정 | 운영 증거 |
| --- | --- | --- |
| 승인된 node 등록과 heartbeat | `PASSED` | camp-1, camp-2, camp-3 모두 approved·online |
| GPU·Docker·NVIDIA runtime 조사 | `PASSED` | 세 노드 모두 Docker ready, NVIDIA runtime ready, RTX 3090 24,576 MiB 보고 |
| 불변 모델·runtime 등록 | `PASSED` | 72B revision, artifact manifest, OCI image digest 고정 |
| 3-way stage variant 생성·검증 | `PASSED` | `TP=1`, `PP=3`, 세 rank 모두 GPU export/load 검증 통과 |
| exact node/GPU/rank qualification | `PASSED` | 8단계 모두 통과, evidence digest 발급 |
| SLO·network 승격 기준 | `PASSED` | 등록된 9개 수치 기준 모두 만족 |
| model release와 placement 승격 | `PASSED` | release·placement 모두 `ACTIVE` |
| Controller 단일-node 배포 폐루프 | `PASSED` | 0.5B apply·start·verify 및 별도 verify 성공, deployment `VERIFIED` |
| 인증형 inference gateway | `PASSED` | 무자격 요청 401, 유효 자격 모델 조회·chat 200 |
| 72B Fleet 추천 가능성 | `PARTIAL` | 0.4.23 읽기 전용 평가에서는 세 rank 후보 선택; 운영 Controller 0.4.22에는 미반영 |
| 72B Fleet-native prepare·apply·verify | `NOT_RUN` | 현재 72B serving은 qualification runtime 재사용 경로 |

## 1. Node와 GPU 인벤토리

관리자 node 목록은 세 노드를 모두 승인·online으로 보고했다.

| node | node UUID | phase | Agent | GPU UUID | VRAM | compute capability |
| --- | --- | --- | --- | --- | ---: | --- |
| camp-1 | `36a8b4c5-9e51-48d7-a805-09f7244f8f77` | `DISCOVERED` | 0.4.22 | `GPU-df391914-2e90-1e78-0e9d-083fbb8d8c6f` | 24,576 MiB | 8.6 |
| camp-2 | `eb3dc982-d15e-4308-b5ff-8267bea2ae6d` | `PLANNED` | 0.4.22 | `GPU-d93af6fa-c4d9-2c3b-dff9-84c0dcd1dbfc` | 24,576 MiB | 8.6 |
| camp-3 | `27a2bbb7-4cc7-4aa1-8258-395c67eafdc4` | `DISCOVERED` | 0.4.22 | `GPU-cee5d82f-6132-0552-6a76-22847d2c7d57` | 24,576 MiB | 8.6 |

각 profile에서 Docker engine과 NVIDIA container runtime은 준비 상태였다. camp-2의 `PLANNED`는
노드 장애나 offline 상태가 아니라, 과거 0.5B deployment generation이 node에 연결된 상태가 남아
있기 때문이다. 현재 workload 목록은 비어 있으므로 이 phase만으로 실행 중 workload를 의미하지는
않는다.

## 2. 72B 공급망과 stage identity

검증된 모델 릴리스는 다음 불변 identity로 고정되었다.

| 항목 | 값 |
| --- | --- |
| model | `qwen2.5-72b-awq` |
| upstream repository | `Qwen/Qwen2.5-72B-Instruct-AWQ` |
| revision | `698703eae6604af048a3d2f509995dc302088217` |
| artifact manifest | `sha256:466a6c85ae235bf83201de02ac76f595ed00ff9944966131ea4a10af1b48180c` |
| quantization | `awq` |
| declared size | 39,680 MiB |
| runtime image | `vllm/vllm-openai@sha256:df2c55e5107afea09ea1a50f9dd96c99ebf97a795334c4d08f691f3d79b2ab12` |
| runtime contract | vLLM 0.9.0, CUDA 12.8, Ampere |

3-way stage variant의 artifact-set digest는
`sha256:45de2ef2d03b3abd9e023e08e8c9ea3c48534ce4ced27665e500aaf1b7133589`이며,
상태는 `VALIDATED`다. loader는 `VLLM_SHARDED_STATE_V1`, topology는 `TP=1`, `PP=3`이다.
세 rank의 GPU export/load evidence가 모두 `PASSED`였고, 실제로 읽은 weight byte 수는 다음과 같다.

| pipeline rank | node | 검증된 weight bytes |
| ---: | --- | ---: |
| 0 | camp-3 | 14,848,239,304 |
| 1 | camp-1 | 12,356,822,784 |
| 2 | camp-2 | 14,390,595,824 |

이 결과는 단순히 원본 파일이 존재한다는 확인이 아니라, 고정된 runtime이 각 rank shard를 GPU에
실제로 load할 수 있음을 확인한 기록이다.

## 3. 3노드 qualification

qualification run `18b6daf2-e621-4388-8c15-08e8eda58fd9`는 2026-07-22
07:39:31 UTC에 `PASSED`로 완료되었다. evidence ID는
`c6c21a5a-141c-5685-9212-be852ee43486`, evidence digest는
`sha256:13e017ea2007f0bfbc74b295d48a8cdf4555819f781933e798c192fb856ababe`다.

정확한 rank 결합은 다음과 같다.

| rank | node | GPU |
| ---: | --- | --- |
| 0 | camp-3 | `GPU-cee5d82f-6132-0552-6a76-22847d2c7d57` |
| 1 | camp-1 | `GPU-df391914-2e90-1e78-0e9d-083fbb8d8c6f` |
| 2 | camp-2 | `GPU-d93af6fa-c4d9-2c3b-dff9-84c0dcd1dbfc` |

다음 8단계가 모두 통과했다.

1. `STATIC_COMPATIBILITY`
2. `CAPACITY_ESTIMATE`
3. `ARTIFACT_READY`
4. `NETWORK_NCCL`
5. `MODEL_LOAD`
6. `SHORT_INFERENCE`
7. `CONTEXT_CONCURRENCY`
8. `RESTART_STABILITY`

model load에는 68.79초가 걸렸으며, restart 1회 뒤에도 검증을 다시 통과했다. qualification은
최대 context 8,192, concurrency 1, input 8,160 tokens, output 32 tokens 조건을 사용했다.

## 4. 승격 기준 비교

| 지표 | 요구 조건 | 측정값 | 판정 |
| --- | ---: | ---: | --- |
| TTFT p95 | ≤ 30,000 ms | 19,190.817 ms | `PASSED` |
| TPOT p95 | ≤ 250 ms | 174.945 ms | `PASSED` |
| E2E p95 | ≤ 45,000 ms | 24,614.100 ms | `PASSED` |
| throughput | ≥ 1.0 token/s | 1.311 token/s | `PASSED` |
| request success ratio | ≥ 0.99 | 1.00 | `PASSED` |
| VRAM headroom | ≥ 10% | 18.197% | `PASSED` |
| node 간 bandwidth | ≥ 2,000 Mbps | 2,413.396 Mbps | `PASSED` |
| RTT | ≤ 2 ms | 0.863 ms | `PASSED` |
| packet loss | ≤ 0.1% | 0% | `PASSED` |

이에 따라 placement `auto-qwen2.5-72b-awq-tp1-pp3-v3`와 model release가 모두 `ACTIVE`가
되었다. promotion evidence digest는
`sha256:b7e0003d060e0e83bb5615d40c87dce02641992d36841ded8cb2d7d5d411affe`다.

## 5. Controller와 Agent의 실제 폐루프

72B qualification과 별도로, deployment
`842b47d0-3e72-587c-af6e-19518ab7bc59`는 camp-2에서 0.5B AWQ 모델을 사용해 다음 operation을
실제로 완료했다.

| operation | 결과 |
| --- | --- |
| `APPLY_DEPLOYMENT` | `SUCCEEDED` |
| `START_DEPLOYMENT` | `SUCCEEDED` |
| apply 과정의 `VERIFY` | `SUCCEEDED` |
| 별도 generation `VERIFY` | `SUCCEEDED` |
| 최종 deployment 상태 | `VERIFIED` |

apply operation은 2026-07-21 19:22:20 UTC, 별도 verify는 19:22:57 UTC에 완료되었다.
이 기록은 Controller가 고정된 계획을 Agent에 전달하고, Agent가 폐쇄형 task를 실행하며,
Controller가 단계별 결과를 모아 최종 상태를 결정하는 기본 배포 경로가 실제로 동작함을 입증한다.
해당 deployment는 이후 명시적으로 중지되었고, 그 STOP task도 성공했다.

전체 task 이력 88건 가운데 73건은 `SUCCEEDED`, 15건은 `FAILED`다. 성공 이력에는 probe 61건,
모델·이미지 준비 각 2건, benchmark 1건, apply/start/verify/stop이 포함된다. 실패 이력은 개발 중
발생한 benchmark 14건과 apply 1건으로 보존되어 있다. 이 보고서는 실패를 삭제하거나 성공으로
환산하지 않는다. 최종 72B 판정은 실패 task 수가 아니라 별도의 불변 qualification evidence와
승격 기준 충족 여부에 근거한다.

## 6. 외부 API 확인의 의미

보존한 72B qualification runtime을 별도 gateway에 연결한 뒤 다음을 확인했다.

- credential 없는 요청은 HTTP 401로 거부됨
- 유효한 credential의 model listing은 HTTP 200
- 유효한 credential의 chat completion은 HTTP 200이며 정상적인 모델 응답 반환

이는 3노드 vLLM head가 실제 요청을 처리하고 인증 경계 뒤에서 외부로 전달될 수 있음을 보완적으로
입증한다. 하지만 gateway는 현재 Dure Controller가 생성한 deployment generation의 일부가 아니므로,
이 성공을 72B Fleet apply 성공으로 기록하지 않는다.

## 7. 현재 남은 공백

다음 항목이 완료되어야 72B 자동 배포 전체를 `PASSED`로 판정할 수 있다.

1. 운영 Controller와 세 Agent를 0.4.23 이상으로 일치시킨다.
2. 관리자 recommendation에 exact 72B stage candidate가 생성되는지 확인한다.
3. recommendation을 명시적으로 accept해 불변 Fleet generation과 GPU 예약을 만든다.
4. `fleet prepare`로 세 rank의 exact cache를 중앙에서 `READY`로 재검증한다.
5. `fleet apply`로 Controller가 3노드 runtime을 기동하게 한다.
6. generation 전체의 API와 rank 상태를 `fleet verify`로 검증한다.

현재 중앙 artifact-cache 목록에서 `READY`로 투영된 것은 과거 0.5B full snapshot이다. 72B stage
bytes는 node probe에서 `PRESENT`로 관측되었지만, 운영 Controller 0.4.22의 추천 경로에서는 이를
재사용 가능한 후보로 완전히 연결하지 못했다. 0.4.23 소스를 실제 DB에 읽기 전용으로 적용한 평가에서는
camp-3, camp-1, camp-2를 rank 0, 1, 2로 선택하고 cache hit 3건, 미충족 최소 조건 0건인 후보가
생성되었다. 이 결과는 0.4.23 수정 방향을 지지하지만 운영 배포 성공 증거는 아니다.

## 재현 가능한 관리자 조회

다음 명령은 host 상태를 바꾸지 않고 핵심 중앙 기록을 다시 확인한다. 관리자 credential은 환경 또는
보호된 설정에서 주입하며 출력이나 문서에 기록하지 않는다.

```bash
dure admin nodes
dure admin node show 36a8b4c5-9e51-48d7-a805-09f7244f8f77
dure admin node show eb3dc982-d15e-4308-b5ff-8267bea2ae6d
dure admin node show 27a2bbb7-4cc7-4aa1-8258-395c67eafdc4
dure admin artifact-cache list
dure admin deployment show 842b47d0-3e72-587c-af6e-19518ab7bc59
dure admin tasks
```

model release, stage variant, qualification evidence의 상세 필드는 동일한 admin credential로 각각의
관리자 GET endpoint를 조회해 대조했다. 이 보고서에는 credential, private URL, 내부 IP, host 경로,
raw prompt 또는 원문 로그를 포함하지 않았다.

## 8. 현재 source 회귀 검증

운영 증적과 별도로, `0.4.23` source checkout을 명시적으로 import하도록 고정한 뒤 767개 단위·통합
테스트를 실행했고 모두 통과했다. 문서 검사도 Markdown 53개에 대해 통과했으며 `git diff --check`도
오류가 없었다.

처음 Python 기본 경로로 실행한 검사는 host에 설치된 `0.4.22` package를 일부 import해 740개 중
3건이 실패했다. 현재 source의 실패로 오인하지 않도록 해당 결과는 수용 결과에서 제외하고,
`PYTHONPATH=src`로 import provenance를 고정해 전체 767개를 재실행했다. 이는 source checkout과
설치 package가 공존하는 운영 호스트에서는 검증 대상 경로를 반드시 고정해야 한다는 점도 보여준다.

## 결론

Dure는 단순한 계획 문서나 mock 수준을 넘어, 실제 세 GPU 노드에서 불변 artifact와 runtime을 결합하고,
stage shard를 load하고, NCCL·추론·context·재시작 조건을 검증한 뒤 72B 릴리스와 placement를
`ACTIVE`로 승격했다. 또한 별도의 실제 deployment를 통해 중앙 task 발행, Agent 실행, API 기동,
검증 결과 회수라는 기본 제어 폐루프도 완주했다.

따라서 **노드 관리, 불변 모델 등록, 다중 노드 qualification, 승격, 단일 세대 배포라는 핵심 설계는
의도대로 작동했다고 판단한다.** 남은 마지막 입증 항목은 현재 검증된 72B runtime을 건드리지 않는
별도 전환 계획 아래, 같은 72B exact stage를 Fleet generation으로 prepare·apply·verify하는 것이다.
