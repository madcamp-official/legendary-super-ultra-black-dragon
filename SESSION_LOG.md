# saem 패키지 설계 세션 로그

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

## 아직 안 한 것 (다음 단계 후보)
1. 5개 role의 실제 로직 이식 — 각 파일에 TODO로 표시됨, 원본 스크립트(camp-59 gateway.py 등)
   내용을 아직 못 받아서 스텁 상태. **지금 검증된 건 배포·역할지정 배관이고, role 내부는 전부 스텁.**
2. camp-1/2/3에 Qwen 72B 서빙 올린 뒤 실제 model name으로 `register-backend` 재등록
3. 토큰(`/etc/saem/token`) 배포는 여전히 수동 — 최초 1회는 SSH 필요
4. qdrant_primary role 검증 (Qdrant 바이너리 있는 노드에서)

## 패키지 구조
```
saem/
  cli.py            # saem agent / saem head start / register / register-backend / status
  agent.py          # 상시 데몬 — POST /role, POST /backend 수신
  head.py           # register(), register_backend() — HTTP push + 로컬 레지스트리
  systemd.py        # saem-role.service 렌더링+enable
  run_role.py        # systemd 진입점 — role.yaml 읽어 role의 run() 호출
  common/
    state.py         # /etc/saem/{role.yaml, head_registry.yaml, backend_registry.yaml, backend.yaml, token}
    config.py         # get_llm_backend() 등
  roles/
    __init__.py       # ROLE_ENTRYPOINTS 매핑
    qdrant_primary.py / retrieval_gateway.py / ingest_coordinator.py / crawler.py / api_proxy.py
```
