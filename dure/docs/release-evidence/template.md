# vX.Y.Z 수용 증적

## 상태

`NOT_RUN`

`PASSED`, `FAILED`, `NOT_RUN` 중 하나만 사용합니다. `NOT_RUN`이면 실행하지 못한 이유와 종료 코드
`77` 여부를 적고, 성공으로 해석하지 않습니다.

## 식별 정보

| 항목 | 값 |
| --- | --- |
| source repository | `madcamp-official/legendary-super-ultra-black-dragon` |
| source tag·commit | `<tag 또는 없음> / <40-char commit>` |
| package version·build commit | `<version> / <build commit>` |
| model·revision·manifest digest | `<값>` |
| runtime OCI image digest | `<sha256:...>` |
| profile | `<TP/PP, backend, cache kind>` |
| inventory identity | `<digest>` |
| 실행 시각·운영자 | `<UTC timestamp> / <역할 또는 익명 ID>` |

## 대상과 전제 조건

- node UUID와 선택 GPU UUID·index: `<비밀값 제외>`
- 사설 network·Ray 포트 정책 확인: `<결과>`
- exact cache `READY`와 image preparation evidence: `<결과>`
- `NOT_RUN`이면 누락된 전제 조건: `<결과>`

## 결과

| 검사 | 상태 | 구조화된 결과 요약 |
| --- | --- | --- |
| 단일 GPU activation 순서 | `<상태>` | `<operation/task ordering>` |
| GPU memory attestation | `<상태>` | `<최소·측정값 요약>` |
| network/NCCL | `<상태>` | `<exact identity와 결과>` |
| distributed model load·최소 추론 | `<상태>` | `<결과>` |
| controller apply·verify | `<상태>` | `<결과>` |

## 판정과 후속 조치

- release/generation 판정: `<통과·중단·재시도>`
- 실패 또는 `NOT_RUN` 후 필요한 명시적 조치: `<내용>`
- 이 기록의 재사용 금지 조건: `<identity drift 조건>`
