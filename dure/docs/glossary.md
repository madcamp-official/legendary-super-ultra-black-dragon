# 용어집

## APT

Debian·Ubuntu 계열에서 package와 의존성을 설치·업데이트하는 package manager입니다. Dure의 현재
APT key는 [현재 APT mirror](apt-distribution.md)의 archive metadata를 인증합니다.

## artifact manifest

모델 파일, 상대 경로, 크기, SHA-256과 청크 관계를 고정한 불변 목록입니다. 모델 이름만으로는
파일 내용을 식별할 수 없으므로 manifest digest를 함께 사용합니다.

## evidence

특정 모델·runtime·profile·노드·GPU 조합에서 실행·검증이 성공 또는 실패했다는 구조화된 기록입니다.
다른 노드 조합의 evidence를 현재 조합의 성공으로 재사용하지 않습니다.

## Fleet

GPU 풀에서 서로 겹치지 않는 여러 독립 배포를 하나의 불변 추천으로 묶은 단위입니다. 추천·조회·수락은
호스트를 변경하지 않으며, `prepare`와 `apply`가 별도 명시 단계입니다.

## FULL_SNAPSHOT

각 노드가 모델 전체 snapshot을 독립 cache로 준비해 소비하는 전달 방식입니다. stage 방식과 같은
추천 안에서 묵시적으로 교체되지 않습니다.

## manifest

배포에 필요한 입력과 identity를 불변으로 기술한 문서 또는 JSON입니다. Dure에서는 model artifact,
stage rank, runtime image와 profile에 따라 서로 다른 manifest가 있습니다.

## NCCL

NVIDIA Collective Communications Library입니다. 여러 GPU·노드가 분산 추론 중 intermediate tensor를
교환할 수 있게 합니다. 각 GPU가 정상이어도 NCCL·network 검증이 실패하면 다중 노드 배포는 허용되지
않습니다.

## PP (Pipeline Parallelism)

모델의 연속된 layer 범위를 pipeline stage로 나누어 여러 GPU가 순서대로 처리하는 방식입니다. 현재
엄격한 다중 노드 runtime은 `PP=2` 또는 `PP=3`만 실행합니다.

## qualification

profile을 `DRAFT`에서 `VALIDATED`, 운영자 `ACTIVE`로 올리기 위한 폐쇄형 검증 절차입니다. 정적
호환성, VRAM·디스크, network/NCCL, 모델 load, 짧은 추론, 성능·안정성 evidence를 결합합니다.

## rank

분산 runtime 안에서 각 worker가 맡는 고유한 순서 번호입니다. pipeline deployment에서는 node UUID와
GPU UUID, private address, PP stage가 함께 고정됩니다.

## STAGE

특정 model·runtime·TP/PP 조합을 위한 rank별 vLLM 입력 artifact입니다. 각 rank worker가 필요한
파일만 읽을 수 있도록 `stages/<pp-rank>` 아래에 분리하며, 원본 checkpoint를 임의로 잘라 놓은 파일과는
다릅니다.

## TP (Tensor Parallelism)

같은 layer의 tensor 계산을 여러 GPU에 나누는 방식입니다. 현재 Fleet와 엄격한 runtime은 항상
`TP=1`이며, 임의 TP 확장은 지원하지 않습니다.

## provenance

특정 package가 어떤 source commit·CI build·release asset에서 만들어졌는지 추적할 수 있는 증명
사슬입니다. APT 서명만으로 canonical source 조직의 승인까지 자동 증명되지는 않습니다.
