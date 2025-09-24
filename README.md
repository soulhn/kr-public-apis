# kr-public-apis

한국에서 제공하는 공공 및 민간 API 활용 코드
쉽게 API를 탐색하고 활용할 수 있도록 돕습니다.

## ⭐ API

- **HRD-Net API** : 직업훈련 과정 조회
- **사람인 API** : 채용정보 데이터 제공

## 📌 포함된 API 설명

- **HRD-NET API** : https://hrd.work24.go.kr/hrdp/ap/papao/PAPAO0100D.do

  - HRDPOA60: 국민내일배움카드 훈련과정API
  - HRDPOA62: 사업주훈련 훈련과정API
  - HRDPOA68: 국가인적자원개발 컨소시엄 훈련과정API
  - HRDPOA69: 일학습병행 훈련과정API

- **사람인 API** : https://oapi.saramin.co.kr/

## 📁 폴더 구조

```
kr-public-apis/
├─ hrd-net/ # HRD-Net API 활용 예제
│ └─ result/ # 결과물 (엑셀 등)
└─ README.md
```

<details>
<summary>📑 Commit Message Convention</summary>

커밋 메시지는 **[타입]: [설명]** 형식을 따릅니다.  
영문 또는 한글 사용 가능하나, **일관성 유지**가 중요합니다.

| 타입 (Type)  | 설명                                                    |
| ------------ | ------------------------------------------------------- |
| **feat**     | 새로운 기능 추가                                        |
| **fix**      | 버그 수정                                               |
| **docs**     | 문서 수정 (README 등)                                   |
| **style**    | 코드 포맷팅, 세미콜론 누락 등 (비즈니스 로직 영향 없음) |
| **refactor** | 코드 리팩토링 (기능 변화 없음)                          |
| **test**     | 테스트 코드 추가/수정                                   |
| **chore**    | 빌드, 패키지 매니저, 환경설정 등 자잘한 변경            |

</details>
