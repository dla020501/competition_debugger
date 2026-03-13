# Submission Debugger (ACCIDENT @ CVPR)

`submission_*.csv`를 역추적해서 비디오 단위 디버깅을 하는 협업용 웹 도구입니다.

## 기능

- 로컬의 `submission_*.csv` 자동 탐색
- 로그인/세션 기반 접근 제어
- 관리자 페이지(사용자 생성/비밀번호 변경/권한 관리)
- 사용자별 기본 source/submission 기억
- 사용자별 submission 접근 권한 강제
- source 고정 경로 선택: `test` / `train`
- submission 간 비교 조회 (A/B/C)
- 전체 submission 스코어보드 계산
- 테스트/학습 비디오 재생 + 예측 좌표/시점 오버레이
- 비디오 재인코딩 없이 브라우저 Canvas로 실시간 오버레이
- 예상 GT(시간/좌표/타입/노트) 협업 편집
- 편집 이력(Audit log) 저장
- 동시 편집 충돌 감지(optimistic lock)
- 예상 GT CSV import/export
- 예상 GT 기준 점수 계산
  - Temporal: `exp(-0.5*(dt/sigma_t)^2)`
  - Spatial: `exp(-0.5*(dist/sigma_s)^2)`
  - Classification: Top-1 (0 또는 1)
  - Final: component mean들의 harmonic mean

> 주의: Kaggle의 내부 metric 구현과 완전 동일하지 않을 수 있습니다. `sigma_t`, `sigma_s`를 조절하면서 상대 비교용으로 쓰는 것을 권장합니다.

## 디렉터리 가정

이 앱은 아래 구조를 기준으로 동작합니다.

- 프로젝트 루트: `cvpr_competition/`
- test 메타데이터: `dataset/test_metadata.csv`
- train 메타데이터: `dataset/sim_dataset/labels.csv`
- test 영상 파일: `dataset/videos/...`
- train 영상 파일: `dataset/sim_dataset/videos/...`
- 제출 파일: `submission_*.csv`

기본적으로 데이터셋은 프로젝트 루트의 `dataset/`을 사용합니다.
다른 위치의 데이터셋을 사용하려면 환경변수 `SD_DATASET_DIR`를 지정하세요.

```bash
export SD_DATASET_DIR=/data/accident_dataset
```

필수 구조:

- `$SD_DATASET_DIR/test_metadata.csv`
- `$SD_DATASET_DIR/videos/...`
- `$SD_DATASET_DIR/sim_dataset/labels.csv`
- `$SD_DATASET_DIR/sim_dataset/videos/...`

## 오버레이 방식

- 서버에서 오버레이된 새 MP4를 만들지 않습니다.
- 원본 비디오는 `/media`에서 그대로 스트리밍합니다.
- 예측/GT 마커와 텍스트는 브라우저 Canvas에서 프레임 위에 즉시 그립니다.
- 따라서 submission을 바꿔도 무거운 비디오 재생성 작업 없이 빠르게 확인할 수 있습니다.

## 실행

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
export SD_DATASET_DIR=/data/accident_dataset   # 필요 시
python3 -m pip install -r requirements.txt
python3 app.py
```

브라우저에서 아래 주소를 열면 됩니다.

- `http://<server-ip>:18080`

학습 서버에서 실행 중이라면 방화벽/보안그룹에서 `18080` 포트를 허용하세요.

## 외부 공개 운영 (권장)

공개 운영은 앱 포트를 직접 노출하기보다 `Nginx -> Uvicorn` 구조를 권장합니다.

### 1) 환경 파일 준비

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
cp .env.example .env
```

`.env`에서 최소 아래 값을 수정하세요.

- `SD_DATASET_DIR`: 실제 데이터셋 루트
- `SD_ADMIN_USER`, `SD_ADMIN_PASS`: 초기 관리자 계정
- `SD_COOKIE_SECURE=1`: HTTPS 환경에서 필수

런타임은 Python 3.10+가 필요합니다. 기본 `python3`가 3.8인 서버라면 `.env`에 `PYTHON_BIN`을 지정하세요.

```bash
PYTHON_BIN=/opt/conda/envs/qwen35/bin/python
```

### 2) 앱 백엔드 시작

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
chmod +x scripts/*.sh
./scripts/start.sh
./scripts/status.sh
curl -sS http://127.0.0.1:18080/healthz
```

### 3) Nginx 리버스 프록시 연결

```bash
sudo cp deploy/nginx_submission_debugger.conf /etc/nginx/sites-available/submission_debugger
sudo ln -s /etc/nginx/sites-available/submission_debugger /etc/nginx/sites-enabled/submission_debugger
sudo nginx -t
sudo systemctl reload nginx
```

`deploy/nginx_submission_debugger.conf`의 `server_name`은 도메인/IP로 바꾸세요.

### 4) 방화벽 정책

- 외부 공개 포트: `80`(또는 `443`)
- 내부 앱 포트: `18080`은 localhost 바인딩 유지 권장

### 5) systemd 자동시작 등록 (선택)

```bash
sudo cp deploy/submission_debugger.service /etc/systemd/system/submission_debugger.service
sudo systemctl daemon-reload
sudo systemctl enable --now submission_debugger
sudo systemctl status submission_debugger
```

`deploy/submission_debugger.service`의 `User`, `Group`, `WorkingDirectory`를 서버 환경에 맞게 수정하세요.

## 운영 절차 (권장)

아래 절차는 팀원이 그대로 따라 쓰기 쉽게 만든 표준 절차입니다.

### 1) 1회 설치

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
/opt/conda/envs/colmap_env/bin/python -m pip install -r requirements.txt
chmod +x scripts/*.sh
cp .env.example .env
```

### 2) 서버 시작

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
./scripts/start.sh
```

### 3) 상태 확인

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
./scripts/status.sh
curl -sS http://127.0.0.1:18080/healthz
```

### 4) 접속

- 로컬: `http://127.0.0.1:18080`
- 원격: `http://<server-ip>:18080`

### 4-1) 로그인

- 로그인 페이지: `/login`
- 기본 계정(최초 1회 자동 생성):
  - ID: `admin`
  - PW: `change-me`

보안을 위해 서버 실행 전 환경변수로 변경을 권장합니다.

```bash
export SD_ADMIN_USER='your_admin'
export SD_ADMIN_PASS='strong_password_here'
./scripts/start.sh
```

### 4-2) 관리자 페이지

- URL: `/admin/users`
- 기능:
  - 사용자 추가
  - 사용자 비밀번호 변경
  - 사용자별 submission 권한 부여/회수

일반 사용자는 자신에게 부여된 submission만 볼 수 있고, API 접근도 동일하게 제한됩니다.

### 5) 로그 확인

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
tail -f data/server.log
```

### 6) 서버 중지

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
./scripts/stop.sh
```

### 7) 문제 발생 시 빠른 복구

```bash
cd /root/Desktop/workspace/competition_debugger/submission_debugger
./scripts/stop.sh
./scripts/start.sh
curl -sS http://127.0.0.1:18080/healthz
```

## 협업 데이터 입출력

### GT 내보내기

- 브라우저에서 `예상 GT CSV Export` 버튼 클릭
- 또는 API 직접 호출: `/api/gt/export`

### GT 가져오기

- 메인 화면에서 CSV 파일 선택 + `GT CSV Import`
- 필수 컬럼: `video_path,accident_time,center_x,center_y,type`
- 선택 컬럼: `note`

예시:

```csv
video_path,accident_time,center_x,center_y,type,note
videos/Z4kg2Ev3vhk_00.mp4,10.5,0.56,0.60,rear-end,reviewed by team
```

## 주요 API

- `GET /api/score?submission=...&sigma_t=2.0&sigma_s=0.15`
- `GET /api/scoreboard?sigma_t=2.0&sigma_s=0.15`
- `POST /api/gt`
- `GET /api/gt/history?video_path=...`
- `GET /api/gt/export`
- `POST /api/gt/import?editor=...`

## 권한 모델

- admin 계정은 모든 submission 접근 가능
- 일반 계정은 관리자 페이지에서 할당한 submission만 접근 가능
- `source=train`도 동일하게 계정별 허용 submission 컨텍스트를 사용

## 협업 운영 팁 (10명 팀 기준)

- 편집자 이름 규칙 통일: 예) `name.initial` (`kim.jh`)
- 매일 1회 DB 백업: `submission_debugger/data/debugger.db`
- 주 단위 스냅샷: 예상 GT를 CSV로 덤프해서 Git에 커밋
- 역할 분담: 영상군/지역별 담당자 지정 후 중복 편집 최소화

## 다음 확장 권장

- 팀/그룹 단위 권한 정책
- 비밀번호 재설정 토큰 메일 연동
- 프레임 단위 썸네일 타임라인 + 키프레임 점프
