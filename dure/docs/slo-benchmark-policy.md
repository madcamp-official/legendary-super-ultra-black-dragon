# SLO·벤치마크 정책 운영 절차

이 문서는 모델 릴리스와 자동 배치 profile의 서비스 수준 목표(SLO)를 정하고, benchmark evidence를
승격·추천에 사용할 수 있는지 판단하는 운영 절차다. 구현된 evidence schema와 상태 전이는
[벤치마크 및 모델 자격 검증](benchmarking.md), exact node·GPU qualification 계약은
[자동 배치 프로필 qualification](profile-qualification.md)을 기준으로 한다.

## 목표와 적용 범위

SLO는 “모델이 실행된다”는 최소 조건과 다르다. Dure는 실행 가능성, quality, TTFT, TPOT, 종단 지연,
유효 처리량, 성공률, VRAM 여유, 재시작 안정성과 다중 노드 network/NCCL 조건을 함께 평가한다.

현재 자동 benchmark는 승인된 단일 GPU node의 `FULL_SNAPSHOT` 경로만 실행한다. 다중 노드
qualification은 신뢰된 외부 executor의 폐쇄형 결과 제출이 필요하다. 이 문서는 다중 노드 benchmark가
자동화되었다고 주장하지 않으며, `NOT_RUN`을 SLO 통과로 해석하지 않는다.

## 역할과 승인 책임

| 역할 | 책임 |
| --- | --- |
| 서비스 소유자 | 사용자 workload, 최소 품질·지연·가용성 목표와 허용 데이터 등급 결정 |
| 모델 관리자 | 모델·revision·runtime·placement profile과 SLO 초안의 일관성 확인 |
| 검증 운영자 | 고정 workload·node binding으로 evidence 실행·등록 |
| 중앙 운영자 | `VALIDATED`·`ACTIVE` 전이, Fleet recommendation·배포 승인 |
| 보안 운영자 | benchmark node가 idle·승인된 network·data boundary에 있는지 확인 |

한 사람이 역할을 겸할 수는 있어도 SLO 변경 승인과 evidence 실행 결과를 같은 기록에서 구분한다.

## SLO 초안 작성

새 모델 릴리스 또는 profile에는 다음을 정규 spec과 release record에 함께 기록한다.

| 범주 | 필수 결정 |
| --- | --- |
| workload | 입력·출력 token 수, 최대 context, 동시성, 예열·최소 측정 횟수, success 기준 |
| 품질 | 승인된 평가 집합·점수 기준·모델별 최소 기준 |
| 지연 | TTFT, TPOT, 종단 지연의 측정 기준과 상한 |
| 처리량 | 유효 처리량·성공률 최소값과 측정 window |
| 자원 | 최소 VRAM·disk, KV cache·작업 공간 여유, node당 GPU 한 장 계약 |
| 안정성 | model load, restart, 오류·OOM 허용 여부와 중단 기준 |
| 다중 노드 | exact node·GPU UUID, interface, RTT·대역폭·loss·NCCL 기준 |

값은 다른 모델·다른 quantization·다른 GPU 조합에서 복사해 통과시키지 않는다. 현재 네 모델의 최소
VRAM·disk와 지원 TP/PP 범위는 [지원 매트릭스](support-matrix.md)를 기준으로 하며, profile의
보수적 하한은 실제 SLO 통과 증거를 대신하지 않는다.

## 기준선 변경 절차

다음 변경은 새 evidence 없이 기존 `PASSED`를 재사용할 수 없는 변경으로 취급한다.

- model revision, manifest digest, quantization, tokenizer·config
- OCI image digest, vLLM/CUDA runtime, Dure build commit
- `TP`, `PP`, context, concurrency, input·output token, warmup·measurement count
- GPU UUID·driver·compute capability, disk/cache kind, node UUID·network interface
- quality·latency·throughput·success·NCCL threshold 또는 측정 방법

1. 서비스 소유자와 모델 관리자가 변경 이유·영향·새 spec digest를 기록한다.
2. 중앙 운영자는 새 또는 수정된 `DRAFT` profile을 만들고, 기존 `ACTIVE` profile·실행 generation을
   묵시적으로 교체하지 않는다.
3. 검증 운영자는 동결된 workload와 exact binding으로 새 evidence를 실행한다.
4. 서버가 현재 inventory·runtime·workload·rank binding을 다시 검사해 `PASSED`로 판정한 경우에만
   `VALIDATED` 전이를 검토한다.
5. 중앙 운영자가 명시적으로 `ACTIVE`로 승격한 뒤에만 새 recommendation 후보가 된다.

SLO를 완화해 과거 실패를 성공으로 바꾸거나, `FAILED` evidence를 지우고 같은 run ID로 다시 보내면
안 된다. 새 policy·workload·request ID·evidence로 처음부터 측정한다.

## 실행과 판정

| 결과 | 의미 | 운영자 행동 |
| --- | --- | --- |
| `PASSED` | 동결 spec과 현재 binding에서 모든 필요한 gate를 통과 | `VALIDATED` 또는 `ACTIVE` 승격을 별도 승인 |
| `FAILED` | SLO 미달 또는 실행 뒤 실패 | 실패 code·metric을 보존하고 원인 수정 후 새 측정 |
| `NOT_RUN` | GPU·runtime·opt-in 등 전제 조건이 없음 | 배포 성공으로 표현하지 않고 전제 조건을 해결하거나 범위에서 제외 |
| `STALE` | identity·inventory·evidence freshness가 현재와 다름 | 새 evidence를 만들어야 recommendation에 사용 가능 |

다중 노드 recommendation은 정확히 같은 node UUID 집합의 최신 `PASSED` network/NCCL evidence만
사용한다. 24시간 TTL은 24시간 지속 실행을 증명하는 값이 아니라 network 측정 재사용 제한이다.

## 운영 기록과 재검토

각 SLO 결정에는 다음의 비밀 없는 기록을 남긴다.

```text
model release / placement profile / spec digest:
workload digest / policy·suite version:
runtime image digest / Dure build commit:
node UUID·GPU UUID·network binding:
SLO 변경 이유와 승인자:
evidence ID·상태(PASSED / FAILED / NOT_RUN):
다음 재검토 시점 또는 변경 trigger:
```

다음 사건이 있으면 즉시 재검토한다: 새 GPU·driver·runtime 배포, cache kind 변경, network 영역 변경,
최근 실행 실패, 품질 회귀, context·concurrency 요구 증가, 모델·라이선스 철회. 로그·prompt·token·원문
stdout/stderr은 이 기록에 넣지 않는다.

## 현재 한계

Dure는 business SLA, 고객별 quota, 장기 availability 목표, 24시간 지속 복구, 전체 workload matrix를
자동 보장하지 않는다. 실제 서비스의 SLO·SLA 공지는 별도 gateway·observability·incident 대응 체계와
측정 증거가 갖춰진 뒤에만 할 수 있다.
