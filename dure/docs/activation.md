# 단일 GPU 릴리스 자동 활성화와 배포

`dure admin activate`는 모델 레지스트리부터 API 검증까지 기존의 폐쇄형 단계를
한 번의 재개 가능한 관리자 흐름으로 연결합니다. 기본 실행은 읽기 전용
미리보기이고 `--apply`를 명시해야 모델·이미지 준비, 벤치마크, 레지스트리 변경과
컨테이너 배포를 시작합니다.

현재 자동 실행 범위는 승인된 온라인 노드 중 하나를 선택하는 `single-gpu`
placement입니다. 모든 온라인 노드를 후보로 줄 수 있지만 최종 배포는 추천기가
고른 한 노드에 생성됩니다. 다중 노드 네트워크·NCCL 측정을 자동 통과로 간주하지
않으며 pipeline placement가 들어간 활성화 문서는 실행 전에 거부합니다.

`single-gpu`의 `PP=1/TP=1` 실행은 digest로 고정한 vLLM 이미지에서 API 컨테이너를
직접 시작하며 Ray 패키지를 요구하지 않습니다. Agent는 기존 결과 호환성을 위해
`ray-container`와 `ray-cluster` 검사 이름을 유지하지만, detail에 Ray 생략을
명시하고 실제 CUDA 검사는 같은 vLLM API 컨테이너 안에서 수행합니다. 이 분기는
`VLLM_RAY_PP_V1` 다중 노드 계획에는 적용되지 않으며 PP=2/3의 Ray·rank 계약을
완화하지 않습니다.

## 사전 조건

- 중앙 서버와 대상 Agent가 모두 0.3.26 이상이어야 합니다.
- 대상 노드는 Docker와 NVIDIA runtime이 준비되고 승인·온라인이어야 합니다.
- runtime 이미지는 `repository@sha256:<digest>` 형식이며 고정한 vLLM·CUDA 실행
  환경을 제공해야 합니다. Agent는 패키지에 포함된 `dure-benchmark`를 읽기 전용으로
  마운트하므로 이미지 자체의 진입점을 신뢰하지 않습니다.
- 모델은 불변 commit revision과 정규 manifest를 가져야 합니다.
- Agent가 HTTPS 중앙 서버에 join했다면 같은 서버의 `/chunks/sha256/`를 기본 신뢰
  origin으로 사용합니다. 별도 origin을 쓰는 경우에만 root 전용
  `/etc/dure/agent.json`의 `artifact_origin`을 설정하며 파일 전체를 출력하거나
  공유하지 않습니다.
- activation 문서의 `benchmark.dure_commit`은 대상 패키지의
  `/usr/share/dure/build-commit`과 정확히 같아야 합니다.

## 문서 형식

최상위 필드는 다음 일곱 개로 고정됩니다. `artifact`와 `runtime`은 불변 레지스트리
입력이고, `manifest`는 `artifact.manifest_digest`와 정확히 일치해야 합니다.
`placement`는 현재 `single-gpu`, `node_count=1`, `PP=1`, `TP=1`, 네트워크·NCCL
요구 없음만 허용합니다. `benchmark.attempt`는 같은 문서로 새 측정을 시작할 때
증가시키는 양의 정수입니다.

```json
{
  "schema_version": 1,
  "artifact": {
    "model_id": "qwen-example-awq",
    "repository": "Qwen/example-AWQ",
    "revision": "<40-64 lowercase hex commit>",
    "manifest_digest": "sha256:<canonical manifest digest>",
    "quantization": "awq",
    "size_mib": 16000,
    "default_max_model_len": 32768,
    "layer_count": 48,
    "license_id": "apache-2.0"
  },
  "manifest": {
    "schema_version": 1,
    "files": [
      {
        "path": "config.json",
        "kind": "REGULAR",
        "size_bytes": 1234,
        "sha256": "sha256:<file digest>",
        "chunks": [
          {
            "ordinal": 0,
            "offset_bytes": 0,
            "length_bytes": 1234,
            "sha256": "sha256:<chunk digest>"
          }
        ]
      }
    ]
  },
  "runtime": {
    "version": "vllm-0.9.0",
    "image": "registry.example/vllm@sha256:<64 lowercase hex>",
    "vllm_version": "0.9.0",
    "cuda_version": "12.4",
    "gpu_architectures": ["ampere"]
  },
  "release": {"quality_rank": 10},
  "placement": {
    "profile_id": "single-24g",
    "topology": "single-gpu",
    "node_count": 1,
    "min_gpu_memory_mib": 24000,
    "min_disk_free_mib": 40000,
    "pipeline_parallel_size": 1,
    "tensor_parallel_size": 1,
    "requires_network_evidence": false,
    "requires_nccl": false,
    "min_bandwidth_mbps": null,
    "max_rtt_ms": null,
    "max_packet_loss_pct": null,
    "max_ttft_p95_ms": 2000.0,
    "max_tpot_p95_ms": 200.0,
    "max_e2e_p95_ms": 10000.0,
    "min_success_rate": 0.95,
    "min_vram_headroom_pct": 5.0,
    "min_throughput_tps": 1.0
  },
  "benchmark": {
    "workload_id": "short-chat-1k-128",
    "dure_commit": "<40-64 lowercase hex Dure build commit>",
    "attempt": 1
  }
}
```

각 file digest와 chunk digest는 실제 origin 바이트의 SHA-256이어야 합니다. 예시의
꺾쇠 값은 그대로 사용할 수 없습니다. 정규 manifest 제약은
[아티팩트 배포 계약](artifact-distribution.md)을 따릅니다.

패키지형 중앙 서버의 기본 origin 디렉터리는
`/var/lib/dure/artifacts/chunks/sha256`입니다. 각 청크는 파일명이 64자리 소문자
SHA-256이고 내용의 SHA-256도 그 이름과 같아야 합니다. 일반 파일, hard link 수 1,
group/other 쓰기 금지 조건을 만족해야 하며 서버는 이 조건을 만족하지 않는 파일을
404로 숨깁니다. 이 경로에는 모델만 두고 credential이나 token을 두지 않습니다.

## 실행

먼저 어떤 노드와 단계를 사용할지 미리 봅니다. 이 명령은 registry, task, Docker를
변경하지 않습니다.

```bash
dure admin activate --file activation.json --all-online

# 또는 후보 UUID를 제한합니다.
dure admin activate --file activation.json \
  --nodes <camp-1-uuid> <camp-2-uuid> <camp-3-uuid>
```

검토 후 전체 흐름을 적용합니다.

```bash
dure admin activate --file activation.json --all-online --apply
```

적용은 다음을 순서대로 수행하며 각 비동기 단계가 성공할 때까지 기다립니다.

1. 후보 노드 PROBE
2. artifact·manifest·runtime·release·placement 멱등 등록
3. exact 모델 준비와 digest 이미지 pull, 고정 단일 GPU 벤치마크
4. 통과 증적을 고정해 `ACTIVE` 승격
5. 최신 PROBE, 추천 생성과 수락
6. 배포 아티팩트 준비, API를 포함한 apply와 verify

성공 출력의 `status`는 `READY`이며 release, recommendation, deployment,
preparation과 verify task ID를 함께 기록합니다. 어느 단계든 실패하거나 시간이
초과되면 뒤 단계를 실행하지 않습니다. SLO 미달은 자동으로 통과시키지 않고 실패
증적으로 남습니다. 원인을 수정한 뒤 같은 불변 입력을 유지하고
`benchmark.attempt`를 증가시켜 다시 실행합니다.

## 자동화 경계

- arbitrary shell, Docker 인자, 환경 변수, mount와 host 경로는 activation 문서에
  표현할 수 없습니다.
- 기본 미리보기는 서버 상태를 변경하지 않습니다.
- `--apply`는 모델 다운로드, 이미지 pull, 격리 벤치마크 컨테이너, 실제 서비스
  컨테이너와 API 검증까지 한 번에 승인합니다.
- 기존 ACTIVE 릴리스와 정확히 같은 입력은 재사용합니다. 같은 불변 identity에 다른
  metadata가 발견되면 덮어쓰지 않고 중단합니다.
- 일반 추천기가 다른 ACTIVE 릴리스를 더 높게 평가하면 그 모델을 대신 배포하지 않고
  중단합니다. activation 대상이 선택되도록 `quality_rank`와 후보 노드를 조정한 뒤
  미리보기부터 다시 실행합니다.
- 다중 노드 pipeline은 신뢰된 네트워크/NCCL 자동 실행기가 추가될 때까지 이 명령의
  범위 밖입니다. 외부 증적을 자동 생성하거나 성공으로 위조하지 않습니다.
