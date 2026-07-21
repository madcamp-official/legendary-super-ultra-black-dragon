# saem 패키지 설계 세션 로그

## ⚠️ 브랜치 정책 (2026-07-21 확정)
**앞으로 커밋은 `saem-standalone` 브랜치에만 한다.** dure 저장소 안에 `saem/` 폴더로 넣었던
`saem` 브랜치는 여기서 동결 — 두 곳에 같은 수정을 중복 반영하는 부담을 없애기 위함.
`saem-standalone`은 orphan 브랜치라 dure 파일도 dure 히스토리도 없고, 루트가 곧 설치 가능한
패키지라 clone 후 바로 `pip3 install .`이 된다.

## 배경
[[saem-rag-vm-plan]] (camp-57~73 VM 17대)의 각 role(Qdrant, gateway, ingest, crawler, api_proxy)이
전부 `nohup ... &`로 흩어져 있어 재부팅 시 수동 재시작이 필요했음. 이를 "설치+역할지정" 패키지로
뽑아내는 설계.

## 결정된 아키텍처

**설치**: 모든 VM에 동일하게 `pip install -e .` (역할 코드는 다 들어가되, 실제로 뭘 실행할지는
role.yaml이 결정)

**역할 지정**: SSH push 방식 대신 **head → agent HTTP push** 방식 채택.
- 이유: 기존에 camp-7의 개인키를 보안상 삭제한 상태([[local-ssh-access]])라, 특정 VM에
  "나머지 VM 접속용 키"를 다시 심는 걸 피하고 싶었음.
- 각 VM: `saem agent` 상시 데몬(9999포트)이 `POST /role` 대기
- head(1대): `saem head start --ip <자기내부IP>` 로 지정 → `saem head register <ip> --role X`로
  각 VM에 신호 전송
- 재부팅 생존: agent가 role 수신 시 systemd 유닛(`saem-role`, Restart=always)을 직접 설치/기동 →
  기존 nohup 문제 해결

**내부망 전제**: 클라우드 VPC라 192.168.0.x 내부 통신은 보안그룹(22/443만 외부 허용) 제약을 안 받음
→ 등록 커맨드는 전부 내부 IP 사용. api_proxy(camp-73)만 예외적으로 외부 443 바인딩 유지.

**LLM 백엔드(dure) 연동**: GPU 클러스터(235B 등)는 별도 패키지 `dure`로 관리되므로 saem을
설치하지 않음. 대신 `saem head register-backend <name> <url> --model <model>`로 head가 URL만
등록/기억하고, `--active`(기본)면 retrieval_gateway/api_proxy 역할 VM들에 자동 전파.
새 GPU 헤드(camp1 등)로 교체 시 이 명령 한 번이면 전 VM 전파, 코드/env 수정 불필요.

## 확정 role 5개 (v1)
qdrant_primary, retrieval_gateway, ingest_coordinator, crawler, api_proxy
(qdrant_replica / embedding_worker / training_pipeline은 실제 로직 없어 보류 — 필요해지면 role
파일 하나 추가하는 식으로 확장)

## 실배포 검증 완료 (2026-07-21, camp-65~69)

VM 5대에 설치하고 전 파이프라인 실동작 확인함. **saem head = camp-65**(VM 클러스터 컨트롤러)이고,
camp-1/2/3 같은 GPU 노드의 "head"(vLLM/Ray head)와는 완전히 별개 개념 — GPU 노드엔 saem을 설치하지 않음.

**설치 함정 (겪은 순서대로):**
- `pip3 install -e .` 실패 — VM의 pip 22.0.2 + setuptools 59.6 조합이 PEP 660 `build_editable` 훅 미지원.
  editable 포기하고 일반 설치로 전환.
- 일반 설치도 `UNKNOWN-0.0.0`으로 빌드됨 — 오래된 setuptools가 `pyproject.toml`의 PEP 621 `[project]`
  테이블을 아예 못 읽음(빌드 격리로 최신 setuptools를 받아와도 동일). **해결: `setup.py`를 추가**하고
  pyproject.toml은 build-system만 남김. 이후 정상 빌드.
- `sentence-transformers` 의존성이 torch를 끌고 와서 설치가 수 분 걸림 — 멈춘 게 아님.
  ⚠️ vLLM이 돌고 있는 GPU 노드에 깔 때는 torch 버전이 바뀔 수 있으니 주의(이번 VM들은 무관).
- 설치 중 `Can't uninstall 'click'/'setuptools' ... outside environment` 경고는 무시해도 됨
  (apt로 깐 시스템 패키지를 pip이 못 건드리는 것뿐, 실패 아님).

**검증 결과:**
- 역할 배정: camp-66=retrieval_gateway(9000), 67=ingest_coordinator, 68=crawler(9200), 69=api_proxy(443).
  네 대 모두 `systemctl status saem-role`이 **active + enabled** — 재부팅 자동 복구 확인(기존 nohup 문제 해결).
- `saem head status`에 head 자신(role: head) + 4대가 모두 표시됨.
- `saem head register-backend camp1-72b http://192.168.0.20:8000 --model qwen-72b` 한 줄로
  **consumer 역할(gateway, api_proxy) 2대에만** backend.yaml이 전파됨을 확인. ingest/crawler엔 안 감(의도대로).
- camp-66 `/ask` 스텁 응답 정상 수신.
- qdrant_primary는 이번 테스트에서 제외 — `/root/qdrant/qdrant` 바이너리가 없는 VM이라 크래시 루프 남.

**포트 매핑(테스트 기준):** 66=9000, 67=없음(HTTP 서버 아님), 68=9200, 69=443.

## role 로직 이식 완료 (2026-07-21)

스텁이던 5개 role에 실제 구현을 넣음. 원본은 기존 노드에서 돌던 nohup 스크립트들이고,
전체 작업 로그(`session-full-log_dure.md`)에 코드가 남아 있어 거기서 복원함.

- `retrieval_gateway` ← camp-59 gateway.py v3: 2단 폴백(repo → web → none), `/ask` + `/vibecutter/ask`
- `crawler` ← camp-18 crawler.py: ddgs + trafilatura
- `ingest_coordinator` ← camp-60 ingest.py: repo pull + 증분 색인
- `api_proxy` ← camp-73 api_proxy.py: Bearer 인증, 스트리밍 중계, jsonl 로깅
- `qdrant_primary`: 바이너리 경로만 config로 분리

**원본 대비 바꾼 것:**
- vLLM 주소 하드코딩 제거 → `get_llm_backend()` 조회. GPU 클러스터 교체해도 코드 수정 불필요.
- ingest는 cron+flock 대신 인프로세스 루프 — systemd가 이미 단일 인스턴스를 보장하므로 락 불필요.
- **`sentence-transformers` → `fastembed`**: 원본이 실제로 쓰던 것. sentence-transformers는 torch(수 GB)를
  끌고 오는데 3GB VM엔 불필요했음.
- `lxml_html_clean`을 의존성에 명시 — 없으면 trafilatura 임포트가 실패해 crawler가 아예 안 뜸.

**검증:** camp-68 crawler가 실제 웹 결과 반환 확인. camp-66 gateway는 Qdrant 검색까지 성공하고
LLM 호출에서 `Connection refused`(camp-1 vLLM 미기동) — 즉 검색 경로는 정상.

## unregister 추가 + 지연 로딩 (2026-07-21)

`saem head unregister <ip>` / `unregister-backend <name>` 추가. 설계 판단 두 가지:
- 노드가 응답 없으면 레지스트리를 **안** 지움(재부팅 중인 VM을 조용히 잃지 않도록). 진짜 폐기는 `--force`.
- 활성 백엔드 해제 시 consumer 노드에도 삭제 신호 전송 — 안 그러면 head는 은퇴시켰는데 노드는 계속 호출함.

**이 과정에서 실제 버그 발견:** `roles/__init__.py`가 5개 role을 전부 즉시 import하고 있어서, 어느 role
하나의 패키지가 없으면 **`saem agent` 자체가 안 뜸**. 역할 배정을 받으려고 존재하는 agent가 그 이유로
죽는 구조였고, head 노드도 쓰지 않는 trafilatura/fastembed를 강제로 깔아야 했음. → `importlib` 지연
로딩으로 전환, 각 노드는 자기 role의 의존성만 있으면 됨.

## 아직 안 한 것 (다음 단계 후보)
1. camp-1/2/3에 Qwen 72B 서빙 올린 뒤 실제 model name으로 `register-backend` 재등록
   (그 전까지는 기존 235B `http://192.168.0.228:8000` / `qwen3-235b`로 붙여 end-to-end 검증 가능)
2. 토큰(`/etc/saem/token`) 배포는 여전히 수동 — 최초 1회는 SSH 필요
3. qdrant_primary role 검증 (Qdrant 바이너리 있는 노드에서)
4. api_proxy는 `/root/api_keys.txt`가 있어야 인증 동작 — 키 파일 배포 방식 미정

## 패키지 구조
`saem-standalone` 브랜치는 루트가 곧 패키지 루트다(clone 후 바로 `pip3 install .`).
```
setup.py          # 메타데이터는 여기 — 지우면 UNKNOWN-0.0.0으로 빌드됨 (위 함정 참고)
pyproject.toml     # build-system만
saem/
  cli.py            # agent / head start·register·unregister·register-backend·status
  agent.py          # 상시 데몬 — POST·DELETE /role, POST·DELETE /backend 수신
  head.py           # register/unregister(_backend) — HTTP push + 로컬 레지스트리
  systemd.py        # saem-role.service 렌더링·enable·제거
  run_role.py        # systemd 진입점 — role.yaml 읽어 role의 run() 호출
  common/
    state.py         # /etc/saem/{role.yaml, head_registry.yaml, backend_registry.yaml, backend.yaml, token}
    config.py         # get_llm_backend(), Qdrant·크롤러 주소, 임베딩 모델 등 전부 env 오버라이드 가능
  roles/
    __init__.py       # ROLE_MODULES + 지연 로딩 (6번째 role 추가 지점)
    qdrant_primary.py / retrieval_gateway.py / ingest_coordinator.py / crawler.py / api_proxy.py
```

## 운영 메모
- 데이터 경로에 head는 없다. 역할 배정이 끝나면 head를 꺼도 서비스는 계속 돈다.
  클라이언트는 gateway(`:9000/ask`)나 api_proxy(`:443/v1/chat/completions`)에 직접 붙는다.
- 6번째 role 추가: `roles/<name>.py`에 `run(port)` 작성 → `ROLE_MODULES`에 한 줄 →
  `common/state.py`의 `ROLE_CHOICES`에 이름 추가. cli/agent/head는 수정 불필요.
