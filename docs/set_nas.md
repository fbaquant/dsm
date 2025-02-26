# NAS Setup

## 구성품

- NAS 본체 1개
- 어댑터
    - 250V 선 1개
    - 시놀로지 어댑터 1개
- 쿨러 2개
- 공유기 1개
- 랜선 3개

## 설치

1. NAS 본체에 전원을 연결하고 랜선을 연결합니다.
2. 설치하고자 하는 컴퓨터에 NAS 데스크톱 앱을 설치합니다.
    - **Synology Assistant**를 다운로드:
      - [Synology Assistant 다운로드 링크](https://www.synology.com/ko-kr/support/download/DS923+?version=7.2#utilities)
3. 설치된 앱을 통해 NAS 장치의 아이피로 접속을 확인합니다.

## 오류 해결

- 오류 메시지가 표시될 경우, 우측 상단의 톱니바퀴 아이콘을 클릭합니다.
- "패스워드 암호화를 지원하지 않는 장치와의 호환성 허용" 옵션을 확인합니다.

## 하드 리셋

1. NAS 장치 뒤쪽의 리셋 버튼을 4초간 누릅니다.
2. 첫 번째 삑 소리 후, 버튼에서 손을 떼고 다시 누릅니다.
    - 총 세 번의 삑 소리가 날 때까지 기다립니다.
3. 리셋이 완료되면 다시 삑 소리가 나고, 기존의 아이피와 포트로 크롬에서 접속합니다.
4. 운영체제가 자동으로 다운로드 및 설치됩니다. 설치 완료까지 약 10분 소요됩니다.
5. 설치 후 NAS 계정으로 로그인하거나 새 계정을 생성합니다.

## QuickConnect

- QuickConnect는 어디서든 NAS 접속을 지원하는 무료 서비스입니다.
- QuickConnect ID: `fbaquant`
    - 웹 브라우저 접속: https://quickconnect.to/{name}
    - 모바일 앱 접속: `{name}`

## 참고 자료

- [NAS 위치를 찾을 수 없는 경우 해결 방법](https://kb.synology.com/ko-kr/DSM/tutorial/Unable_to_Locate_NAS)
- [Synology Assistant 사용 설명서](https://kb.synology.com/ko-kr/DSM/help/Assistant/assistant?version=7#preferences)