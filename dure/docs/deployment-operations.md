# 배포·Fleet 운영

이 문서는 qualification, recommendation, Fleet, deployment generation, verify, rollback의 운영 순서를
다룹니다. 현재 허용 모델·VRAM·TP/PP는 [지원 매트릭스](support-matrix.md)를 기준으로 합니다.

## 배포 전 확인

1. 대상 node가 승인·온라인 상태인지 확인합니다.
2. 선택 GPU UUID·index, runtime, disk, 모델·이미지 cache identity가 현재 인벤토리와 일치하는지 확인합니다.
3. 다중 노드는 exact node·GPU·runtime·profile·network/NCCL evidence가 최신인지 확인합니다.
4. profile과 모델 릴리스가 `ACTIVE`인지 확인합니다.

추정 VRAM, 다른 node 집합의 증적, 오래된 recommendation으로는 다중 노드 배포를 통과시키지 않습니다.

## qualification과 benchmark

- 자동 profile은 `DRAFT → QUALIFYING → VALIDATED → ACTIVE`를 따릅니다.
- 현재 다중 노드 qualification 실행기는 신뢰된 외부 executor의 폐쇄형 evidence 제출 계약입니다.
- 자동 `BENCHMARK`는 단일 GPU의 exact `FULL_SNAPSHOT`만 지원하며, `STAGE`·다중 노드 backend를
  성공 경로로 사용하지 않습니다.
- `NOT_RUN`은 통과가 아닙니다. 실제 GPU 실행 뒤 실패는 `FAILED`로 기록합니다.

자세한 상태 전이와 SLO 기준은 [모델 선택 정책](model-selection.md),
[qualification](profile-qualification.md), [벤치마크](benchmarking.md),
[SLO·벤치마크 정책](slo-benchmark-policy.md)을 따릅니다.

## recommendation과 Fleet

`recommend`와 `accept`는 불변 스냅샷·예약·generation을 만들 수 있지만, 모델 다운로드·이미지 pull·Docker
컨테이너 시작은 하지 않습니다. 동일 인벤토리·정책에서는 결정론적으로 같은 추천을 만들어야 합니다.

```bash
dure admin fleet recommend --all-online --objective quality-first
dure admin fleet show <recommendation-id>
dure admin fleet accept <recommendation-id>
```

수락 시 node·GPU 예약은 전체 transaction으로 검사합니다. 충돌하면 일부 deployment만 생성하지 않습니다.

## 준비·적용·검증

1. `prepare` preview로 task가 생기지 않는지와 exact artifact·image identity를 확인합니다.
2. 운영자가 명시적으로 apply해 모델 cache와 digest-pinned image를 준비합니다.
3. 모든 대상의 exact cache가 `READY`이고 현재 준비 증적이 있을 때만 deployment/Fleet apply를 실행합니다.
4. 전체 배정 node의 `VERIFY`와 strict rank·API 검증 결과를 확인합니다.

준비 실패·apply 실패 뒤에는 자동 rollback, 자동 node 재배정, 자동 캐시 삭제를 가정하지 않습니다.

## 명시적 rollback

rollback은 검증된 직접 직전 generation과 동일한 전체 토폴로지에서만 수행합니다. 대상 모델을 다시
다운로드하거나 이미지를 pull하지 않으며, source를 중지한 뒤에도 target의 exact `READY`·image 준비
증적을 다시 확인합니다. 같은 GPU에서 컨테이너를 다시 만드는 방식이므로 중단 가능성이 있고 blue/green
전환이 아닙니다.

상세 명령·failure code·재시도 fencing은 [통합 운영 참조](operations-reference.md), Fleet 상태는
[Fleet 스케줄러](fleet-scheduler.md), API 요청은 [API 계약](api-contract.md)을 확인합니다.
