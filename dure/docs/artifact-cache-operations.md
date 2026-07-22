# 아티팩트·캐시 운영

이 문서는 모델 manifest, `FULL_SNAPSHOT`, rank별 `STAGE`, 중앙 준비, cache 상태와 격리 절차를 다룹니다.
파일 형식·identity 계약은 [아티팩트 배포 계약](artifact-distribution.md)과
[stage 아티팩트](stage-artifacts.md)를 기준으로 합니다.

## 반입과 manifest

모델은 repository 이름만이 아니라 immutable revision, 정규 manifest, SHA-256, 양자화, 라이선스,
runtime OCI digest를 함께 승인합니다. allowlist 밖 모델이나 mutable revision을 임의 등록하지 않습니다.
정책·승인 책임은 [모델 반입 정책](model-onboarding-policy.md)을 따릅니다.

## 준비와 cache 상태

중앙 준비는 preview와 explicit apply를 분리합니다. Agent는 신뢰된 HTTPS origin과 task-scoped manifest로만
청크를 내려받고, CAS·전체 파일·tree·marker를 검증한 뒤 marker-last와 no-replace rename으로 활성화합니다.

| 상태 | 운영 의미 |
| --- | --- |
| `READY` | 현재 준비 시도의 exact identity가 성공했음. 장기 GPU 안정성 증거는 아님 |
| `STALE` | 최신 identity 또는 variant 신뢰가 바뀌어 재준비가 필요함 |
| `MISSING` | 완전한 probe에서 cache가 없다고 확인됨 |
| `CORRUPT` | 무결성 또는 실행 검증 실패로 소비를 차단함 |
| `QUARANTINED` | 참조 없는 exact cache를 보존 영역으로 명시 이동함 |

불완전 probe나 늦은 과거 완료는 `READY`를 만들거나 되살리지 못합니다.

## STAGE와 다중 노드

`STAGE`는 검증된 source·runtime·TP/PP 계약에서 rank별 vLLM 입력을 준비하는 형식입니다. schema는 넓은
PP identity를 표현할 수 있어도 현재 runtime은 `TP=1`, `PP=2/3`만 지원합니다. rank별 `STAGE`는
정확한 node UUID·rank·manifest·cache identity와 최신 evidence에 결합됩니다.

## 격리와 실패 대응

`artifact-cache list`, `show`, `verify`는 읽기 전용입니다. 격리는 preview 뒤에만 명시 적용합니다.

```bash
dure admin artifact-cache quarantine <cache-id>
dure admin artifact-cache quarantine <cache-id> --apply
```

활성 deployment, 준비 operation, 실행 task, 현재 generation, 검증된 rollback 선행 세대가 참조하면
격리를 거부합니다. 자동 eviction·삭제·P2P 전송·복구는 수행하지 않습니다. disk·I/O·digest 실패는
원인을 해결한 뒤 동일 identity를 다시 준비하며, 수동 경로 복제나 임의 Docker mount로 우회하지 않습니다.

실패 코드와 low-level 복구 순서는 [통합 운영 참조](operations-reference.md), 보존·삭제 승인 기준은
[데이터 보존 정책](data-retention.md)을 따릅니다.
