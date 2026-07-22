# 기여 안내

Dure에 기여해 주셔서 감사합니다. 이 저장소의 코드는 [Apache License 2.0](LICENSE)으로 제공됩니다.
별도 계약이 없는 한 pull request, issue comment, 문서·테스트·코드 제출은 이 라이선스 조건으로
기여한 것으로 간주합니다.

## 기여 전 확인

- Dure source는 `dure/` 아래에 있습니다. 먼저 [Dure README](dure/README.md),
  [문서 색인](dure/docs/README.md), [개발·릴리스 절차](dure/docs/development.md)를 읽습니다.
- 보안 취약점, credential, token, private model URL, 개인정보, 실제 prompt는 public issue나 PR에
  올리지 않습니다. 취약점은 [보안 정책](SECURITY.md)을 따릅니다.
- 기능 문서는 현재 구현과 계획을 구분합니다. 아직 없는 gateway, 자동 multi-node qualification,
  public API 기능을 현재 제공 기능으로 표현하지 않습니다.

## 권장 절차

1. `main`의 최신 상태에서 독립 branch를 만듭니다. 기존 release line 작업은 해당 `version/<semver>`
   branch에서 계속합니다.
2. 변경 범위를 작게 유지하고, 코드·CLI·API·schema·운영 절차를 바꾸면 관련 문서를 같은 PR에서 갱신합니다.
3. credential, `.env`, `/etc/dure/agent.json`, `/etc/dure/server.env`, signing key, model token, 생성된
   package를 commit하지 않습니다.
4. commit은 의도를 드러내는 짧은 메시지로 만들고, PR 본문에는 변경 이유, 동작 영향, 제한 사항,
   검증 결과를 적습니다.

## 검증

문서만 바꿔도 링크와 whitespace를 확인합니다.

```bash
cd dure
python3 scripts/check_docs.py
python3 -m unittest tests.test_docs_check -v
git diff --check
```

코드, packaging, migration, entry point를 바꾸면 다음도 실행합니다.

```bash
cd dure
python3 -m compileall -q src tests
python3 -m unittest discover -v
dure-server --database-url sqlite:////tmp/dure-migration-check.db --migrate
python3 -m pip wheel . --no-deps --no-build-isolation -w /tmp/dure-wheel-check
```

실제 GPU·Docker·NCCL 검증은 unit test를 대신하지 않으며, 실행했다면 결과를
`dure/docs/release-evidence/`에 `PASSED`, `FAILED`, `NOT_RUN`으로 기록합니다.

## 리뷰 기준

- Controller가 node에 inbound SSH를 요구하지 않는지
- pending node가 승인 전 task를 받을 수 없도록 유지되는지
- task payload에 arbitrary shell, Docker option, environment, mount, host path가 추가되지 않는지
- image가 OCI digest로 고정되고 NVIDIA host driver를 자동 변경하지 않는지
- secret·prompt·원문 log가 코드, 문서, test fixture, CI output에 추가되지 않는지
- 변경한 CLI/API/lifecycle/security boundary의 문서와 failure path test가 함께 있는지

행동 기준은 [행동강령](CODE_OF_CONDUCT.md), 사용·지원 범위는 [지원 정책](SUPPORT.md)을 따릅니다.
