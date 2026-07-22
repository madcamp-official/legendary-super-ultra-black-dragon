# 지원 정책

## 지원 범위

Dure는 현재 신뢰된 운영자와 사설 network를 위한 CLI, Agent, 선택형 Control Plane MVP입니다.
공개 GPU network, 최종 사용자 inference gateway, API key·quota·billing, WireGuard 자동화, 상용 SLA는
제공하지 않습니다. 지원되는 OS·package·모델·runtime 범위는
[지원 매트릭스](dure/docs/support-matrix.md)를 기준으로 합니다.

## 도움 요청 방법

bug, 문서 오류, 재현 가능한 설치·운영 문제는 GitHub issue에 다음 정보를 넣습니다.

```text
Dure version 또는 source commit:
OS·architecture·Docker/NVIDIA runtime의 공개 가능한 버전 정보:
수행한 명령과 redaction된 출력:
기대한 동작 / 실제 동작:
이미 확인한 문서·검증 단계:
```

`DURE_ADMIN_TOKEN`, database URL, enrollment token, node credential, APT private key, model access token,
private host address, prompt·completion, raw container log는 넣지 않습니다. 보안 문제는
[보안 정책](SECURITY.md)으로 신고합니다.

## 지원 수준

이 저장소는 best-effort community support를 제공합니다. 응답 시점, uptime, compatibility, managed
operation을 보장하지 않습니다. 운영 전에는 [운영 절차](dure/docs/operations.md),
[네트워크·방화벽 운영 절차](dure/docs/networking.md), [재해 복구](dure/docs/disaster-recovery.md)를
검토하고, 실제 GPU/NCCL 결과는 release evidence로 별도 기록합니다.
