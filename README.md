# 🚇 Seoul Transit Data Pipeline

서울 열린데이터광장의 **서울시 대중교통 이용패턴 정보**를 API를 통해 수집하고, Apache Airflow를 활용하여 **증분 업데이트** 및 **스케줄링** 자동화를 구현한 데이터 파이프라인 프로젝트입니다.

---

## 📌 프로젝트 개요
<img width="891" alt="Screenshot 2025-05-29 at 11 54 07 AM" src="https://github.com/user-attachments/assets/ee920772-e9cf-44e2-85b2-c3501144dc02" />
<br>

- **데이터 출처**: 서울 열린데이터광장 (https://data.seoul.go.kr/)
- **수집 방식**: RESTful API 호출(ksccPatternStation)
- **자동화 도구**: Apache Airflow(Docker 환경)
- **주요 기능**:
  - API 호출을 통한 **서울 지하철 이용 현황** 데이터 수집
  - **증분 방식으로 업데이트**되도록 파이프라인 설계
  - Airflow로 **일일 스케줄링 및 실패 자동 재시도 설정**
  - Docker Compose 기반의 손쉬운 배포 및 관리
  - 수집된 데이터를 **CSV, DB, 또는 S3 등 저장소로 저장** (선택적 확장 가능)
  
---

## 🛠️ 기술 스택

| 기술 | 설명 |
|------|------|
| Python | 데이터 수집 및 처리 로직 |
| Apache Airflow | 워크플로우 자동화 |
| Docker / Docker Compose | 컨테이너 기반 환경 구성 |
| Requests | API 호출 |
| Pandas | 데이터 처리 |
| Git | 버전 관리 |


---

## 📁 프로젝트 구조

```bash
seoul-subway-pipeline/
├── dags/
│ └── fetch_seoul_subway_data.py # Airflow DAG 정의
├── data/
│ └── subway_YYYYMMDD.csv # 수집된 데이터 저장 (예시)
├── requirements.txt # 필요 패키지 목록
├── Dockerfile / docker-compose.yml (선택)
└── README.md
```

---

## 🔄 주요 기능

### ✅ API 호출 및 증분 수집
- 서울시 지하철 호선별 시간대별 이용 현황 API 호출
- 마지막 수집 일자를 기준으로 이후 데이터만 수집
- 중복 제거 및 날짜 기준 필터링

### ✅ Apache Airflow DAG
- `@daily` 스케줄로 자동 실행
- `start_date` 및 `catchup` 옵션 설정
- Task 실패 시 재시도 설정

### ✅ Docker 환경 구성
- Airflow, Scheduler, Webserver, Postgres, Redis 컨테이너 포함
- `.env` 파일로 포트 및 설정값 관리
- 명령어 하나로 실행 가능

---

## 🚀 실행 방법

### 1. 저장소 클론 및 환경 설정

```bash
git clone https://github.com/mscsy0104/docker-seoul-transit-pipeline.git
cd docker-seoul-transit-pipeline
```

### 2. Docker Compose 실행

```bash
docker-compose up --build
```
- Airflow UI: http://localhost:8080

- 기본 로그인 정보:

  - ID: airflow

  - PW: airflow

### 3. DAG 확인 및 실행

웹 UI에서 fetch_transits DAG를 활성화

DAG는 매일 자동 실행되며, 수집된 데이터는 data/ 폴더에 초기에는 전체 업데이트, 이후에는 중분 업데이트 됩니다.

---

### 📌 향후 개선 사항
PostgreSQL, S3, BigQuery 등 외부 저장소 연동

- 데이터 유효성 검증 및 알림 기능 추가

- 시각화 도구 (Tableau, Superset 등) 연동

- 다양한 날짜 필터/에러 핸들링 로직 강화

---

### 📄 참고 자료
- 서울 열린데이터광장

- Apache Airflow 공식 문서

- Docker Compose 문서

