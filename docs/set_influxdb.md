# InfluxDB Setup

## 설치

### 1. Docker 설치

Synology NAS에서 패키지 센터를 열고, 검색창에 "Docker"를 입력한 후 Docker(Container Manager)를 설치합니다.

### 2. InfluxDB 이미지 다운로드

Docker 애플리케이션을 실행하고, '레지스트리' 탭에서 "InfluxDB"를 검색합니다. 공식 이미지를 선택하고, 사용하고자 하는 버전의 태그를 선택하여 다운로드합니다.

### 3. 컨테이너 생성

'이미지' 탭으로 이동하여 다운로드한 이미지를 바탕으로 새로운 InfluxDB 컨테이너를 생성합니다.  
다음과 같은 설정을 구성합니다:

- **네트워크**: 컨테이너의 `8086` 포트를 NAS의 동일 포트로 포워딩합니다.
- **볼륨**: 데이터와 설정 파일을 저장할 NAS 내의 경로를 지정합니다.
  - 예: NAS의 `/docker/influxdb/data`를 컨테이너의 `/var/lib/influxdb`에 매핑
  - 설정 파일 경로: NAS의 `/docker/influxdb/config`를 컨테이너의 `/etc/influxdb`에 매핑

### 4. 컨테이너 실행

모든 설정이 완료되면, 컨테이너를 실행합니다. 이제 InfluxDB가 작동을 시작하며, 설정한 포트를 통해 접근할 수 있습니다.

### 5. 보안 설정

Synology DSM의 제어판을 통해 방화벽 설정에서 허용된 IP와 포트만 접근하도록 구성하여 보안을 강화합니다.

## InfluxDB 접근

InfluxDB는 HTTP API를 통해 접근할 수 있으며, 기본적으로 포트 8086을 사용합니다.

1. **웹 인터페이스 접속**: 브라우저에서 `http://<NAS-IP-주소>:8086`으로 접속하여 InfluxDB의 웹 인터페이스에 접근할 수 있습니다. 처음 접속 시에는 사용자 설정을 진행해야 할 수도 있습니다.

2. **CLI 접근**: Synology NAS에서는 Docker 컨테이너의 터미널에 접근하여 InfluxDB CLI를 사용할 수 있습니다. DSM의 Docker 애플리케이션에서 컨테이너를 선택하고 '터미널' 버튼을 클릭한 후, 새 터미널 세션을 시작하여 `influx` 명령어를 실행합니다.

### 데이터베이스 생성

InfluxDB에서 데이터베이스를 생성하기 위해 다음 CLI 명령어를 사용할 수 있습니다:

```bash
CREATE DATABASE mydb
```

## 참고

- [InfluxDB 공식 문서](https://docs.influxdata.com/influxdb/)
- [Synology 지원 센터](https://www.synology.com/support)
