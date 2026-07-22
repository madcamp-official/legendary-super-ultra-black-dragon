# Dure 제품 제안서: 현재 범위와 장기 비전

이 문서는 현재 Dure가 제공하는 운영 계약과 장기 제품 비전을 분리하는 짧은 안내서입니다.
실행 가능한 기능은 코드·지원 매트릭스·runbook을 기준으로 판단하며, 장기 비전은 현재 기능이나
공개 서비스 약속이 아닙니다.

## 현재 제공 범위

- Linux GPU 노드의 로컬 bootstrap·진단과 운영자 명시 승인 기반 등록
- 네 Qwen2.5 AWQ 모델의 제한된 배치 profile, evidence gate, Fleet 추천·수락
- digest 고정 이미지와 exact 모델 artifact를 사용하는 준비·배포·검증·명시적 rollback
- `TP=1`과 단일 GPU 또는 검증된 `PP=2/3` 계약의 제한된 Ray/vLLM 실행

현재 지원 OS·모델·GPU·TP/PP·증적 조건은 [지원 매트릭스](support-matrix.md), 실제 운영 순서는
[운영 절차](operations.md), 실제 GPU/NCCL 실행 여부는 [릴리스 증적](release-evidence/README.md)을
따릅니다. `NOT_RUN`은 성공 증거가 아닙니다.

## 현재 제공하지 않는 범위

다음 기능은 Dure의 현재 공개 지원 범위가 아닙니다.

- 최종 사용자용 OpenAI 호환 Gateway, API key, quota, billing
- data parallel, 임의 `TP`·`PP`, 이기종 GPU를 임의로 결합하는 실행
- 공개 참여 노드, 자동 failover·자동 rollback, credit ledger·기여자 console
- 민감 데이터 처리를 보장하는 다중 운영자 공개 추론 서비스

공개 API를 시작하려면 [외부 추론 API 경계](external-inference-boundary.md)의 별도 gateway·인증·rate
limit·감사 조건을 충족해야 합니다.

## 장기 비전

공유 GPU 기반 커뮤니티 추론 API의 문제 정의, MVP 가설, 기여자·사용자 모델, 장기 로드맵과 성공 지표는
[Community LLM Mesh 장기 비전](vision/community-llm-mesh.md)에 보존합니다. 이 문서는 제안·검토용이며,
현재 기능 문서와 충돌할 때는 현재 지원 계약이 우선합니다.
