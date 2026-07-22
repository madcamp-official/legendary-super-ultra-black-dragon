# 릴리스 수용 증적 기록

이 디렉터리는 실제 GPU·Docker·Ray·vLLM·NCCL 환경에서 실행한 수용 검사의 결과를 version별로
보관합니다. [릴리스 수용 검증](../release-validation.md)은 절차(runbook)이고, 이 디렉터리는 실제
결과(evidence)입니다.

## 기록 규칙

- 파일명은 `vX.Y.Z.md` 형식을 사용합니다.
- 상태는 `PASSED`, `FAILED`, `NOT_RUN` 중 하나를 첫 부분에 명시합니다.
- `NOT_RUN(77)`은 성공이 아니라 전제 조건이 충족되지 않았다는 기록입니다.
- source commit, package version, model manifest digest, runtime image digest, node UUID와 GPU UUID,
  실행 시각과 구조화된 결과 요약을 기록합니다.
- credential, token, private URL, raw prompt, Docker command, host path, 원본 로그는 기록하지 않습니다.
- model·runtime·node/GPU·profile·inventory identity가 달라지면 과거 `PASSED`를 새 배포의 증거로
  재사용하지 않습니다.

새 기록은 [template.md](template.md)를 복사해 만듭니다.
