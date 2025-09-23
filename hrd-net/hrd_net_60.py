# work24_poa60_ncs20_dynamic_shard.py
import math, time, random, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd, requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

#국민내일배움카드_202401_202508_NCS정보통신_171,052건
# ■ 실행/출력
# - 기간: START_DATE~END_DATE, NCS 1차(srchncs1=20, 정보통신)
# - 월 단위 샤딩 수집, 월 페이지가 1000 초과 시 해당 월만 주간 샤딩으로 세분화
# - 멀티스레드(ThreadPoolExecutor)로 월 샤드 병렬 수집 (MAX_WORKERS로 조절)
# - 결과: 실행 시각 기반 파일명 '{YYMMDDHHMM}_poa60_ncs20.xlsx' 생성
#   - Sheet 'data'    : 원본/정제 데이터 (중복 제거 후)
#   - Sheet 'summary' : 수집 메타 정보 요약


# ===== 사용자 설정 =====
# ===== 인증키 로드 (환경변수/.env) =====
import os
try:
    from dotenv import load_dotenv  # pip install python-dotenv (선택)
    load_dotenv()
except Exception:
    pass

AUTH_KEY = os.getenv("HRDNET_AUTH_KEY")
if not AUTH_KEY:
    raise RuntimeError(
        "환경변수 HRDNET_AUTH_KEY 가 설정되어 있지 않습니다. "
        "레포에 키를 넣지 말고, OS 환경변수 또는 .env 파일로 설정하세요."
    )


START_DATE = dt.date(2024, 1, 1)
END_DATE   = dt.date(2025, 8, 31)
PAGE_SIZE  = 100
MAX_WORKERS= 6                 # 월 샤드 병렬 수(5~6 권장)
USER_AGENT = "HRDNET-Collector/1.0"

# HRDPOA60: 국민내일배움카드 훈련과정
BASE_URL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA60/HRDPOA60_1.jsp"
BASE_PARAMS = {
    "authKey": AUTH_KEY,
    "returnType": "XML",
    "outType": "1",
    "pageSize": str(PAGE_SIZE),
    "sort": "ASC",
    "sortCol": "2",       # 1:기관명, 2:시작일, 3:기관 직종별 취업률, 4:만족도
    "srchNcs1": "20",     # ✅ NCS 1차: 정보통신
    # 전체 유형 수집: crseTracseSe 미등록(필요 시 아래 예시처럼 지정)
    #"crseTracseSe": "C0061I",
}

# ===== 공통 유틸 =====
def new_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=30, pool_maxsize=30)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})
    return s

def ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def month_shards(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    end_m = dt.date(end.year, end.month, 1)
    while cur <= end_m:
        last = (dt.date(cur.year, 12, 31) if cur.month==12
                else dt.date(cur.year, cur.month+1, 1) - dt.timedelta(days=1))
        yield max(cur, start), min(last, end)
        cur = (dt.date(cur.year+1, 1, 1) if cur.month==12
               else dt.date(cur.year, cur.month+1, 1))

def week_shards(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        w_end = min(cur + dt.timedelta(days=6), end)
        yield cur, w_end
        cur = w_end + dt.timedelta(days=1)

def fetch_xml(session: requests.Session, params: dict, page_num: int) -> BeautifulSoup:
    p = params.copy()
    p["pageNum"] = str(page_num)
    r = session.get(BASE_URL, params=p, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.content, "lxml-xml")

def parse_rows_xml(soup: BeautifulSoup):
    out = []
    sl = soup.find("srchList")
    if not sl:
        return out
    for scn in sl.find_all("scn_list"):
        def g(tag):
            el = scn.find(tag)
            return el.text.strip() if el and el.text else ""
        out.append({
            "title": g("title"),
            "subTitle": g("subTitle"),
            "titleLink": g("titleLink"),
            "subTitleLink": g("subTitleLink"),
            "titleIcon": g("titleIcon"),
            "traStartDate": g("traStartDate"),
            "traEndDate": g("traEndDate"),
            "address": g("address"),
            "telNo": g("telNo"),
            "instCd": g("instCd"),
            "trainstCstId": g("trainstCstId"),
            "trainTarget": g("trainTarget"),
            "trainTargetCd": g("trainTargetCd"),
            "trngAreaCd": g("trngAreaCd"),
            "trprId": g("trprId"),
            "trprDegr": g("trprDegr"),
            "ncsCd": g("ncsCd"),
            "yardMan": g("yardMan"),
            "courseMan": g("courseMan"),
            "realMan": g("realMan"),
            "regCourseMan": g("regCourseMan"),
            "grade": g("grade"),
            "contents": g("contents"),
            "certificate": g("certificate"),
            # HRDPOA60 전용(만족도/취업률 등)
            "stdgScor": g("stdgScor"),
            "eiEmplCnt3": g("eiEmplCnt3"),
            "eiEmplRate3": g("eiEmplRate3"),
            "eiEmplRate6": g("eiEmplRate6"),
            "eiEmplCnt3Gt10": g("eiEmplCnt3Gt10"),
        })
    return out

# ===== 핵심: 월→주 동적 샤딩 수집 =====
def collect_one_month(idx: int, m_start: dt.date, m_end: dt.date) -> pd.DataFrame:
    sess = new_session()
    params = BASE_PARAMS.copy()
    params["srchTraStDt"], params["srchTraEndDt"] = ymd(m_start), ymd(m_end)

    # 1) 월 단위 총 페이지 확인
    soup1 = fetch_xml(sess, params, 1)
    scn_cnt = int((soup1.find("scn_cnt").text if soup1.find("scn_cnt") else "0") or 0)
    total_pages = math.ceil(scn_cnt / int(BASE_PARAMS["pageSize"])) if scn_cnt else 0
    print(f"[M{idx:02d}] {m_start} ~ {m_end} | count={scn_cnt:,} | pages={total_pages}")

    if total_pages == 0:
        return pd.DataFrame()

    # 2) 월 단위로 충분하면 그대로 수집
    if total_pages <= 1000:
        rows = parse_rows_xml(soup1)
        for pg in range(2, total_pages + 1):
            time.sleep(0.03 + random.random()*0.07)
            soup = fetch_xml(sess, params, pg)
            rows.extend(parse_rows_xml(soup))
        return pd.DataFrame(rows)

    # 3) ✅ 월 페이지가 1000 초과 → 이 달만 주간 샤딩으로 세분화
    frames = []
    for w_idx, (w_start, w_end) in enumerate(week_shards(m_start, m_end), start=1):
        pw = BASE_PARAMS.copy()
        pw["srchTraStDt"], pw["srchTraEndDt"] = ymd(w_start), ymd(w_end)
        s1 = fetch_xml(sess, pw, 1)
        w_cnt = int((s1.find("scn_cnt").text if s1.find("scn_cnt") else "0") or 0)
        w_pages = math.ceil(w_cnt / int(BASE_PARAMS["pageSize"])) if w_cnt else 0
        print(f"  [M{idx:02d}-W{w_idx}] {w_start}~{w_end} | count={w_cnt:,} | pages={w_pages}")

        if w_pages == 0:
            continue

        w_rows = parse_rows_xml(s1)
        for pg in range(2, w_pages + 1):
            time.sleep(0.03 + random.random()*0.07)
            s = fetch_xml(sess, pw, pg)
            w_rows.extend(parse_rows_xml(s))
        frames.append(pd.DataFrame(w_rows))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def main():
    months = list(month_shards(START_DATE, END_DATE))
    print(f"Total months: {len(months)}")

    # 월 샤드를 병렬 수집
    month_frames = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(collect_one_month, i, m_start, m_end): (i, m_start, m_end)
                for i, (m_start, m_end) in enumerate(months, start=1)}
        for fut in as_completed(futs):
            i, m_start, m_end = futs[fut]
            try:
                df = fut.result()
                month_frames.append(df)
            except Exception as e:
                print(f"[M{i:02d}] failed: {e}")

    ts = dt.datetime.now().strftime("%y%m%d%H%M")
    out_xlsx = f"{ts}_poa60_ncs20.xlsx"

    if not month_frames or all(len(df)==0 for df in month_frames):
        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xw:
            pd.DataFrame().to_excel(xw, sheet_name="data", index=False)
            pd.DataFrame([
                ["API", "HRDPOA60_1 (국민내일배움카드)"],
                ["기간 From", START_DATE.isoformat()],
                ["기간 To", END_DATE.isoformat()],
                ["NCS 1차", "20 (정보통신)"],
                ["pageSize", PAGE_SIZE],
                ["샤딩", "월 → 1000 초과 시 주간"],
                ["총 레코드(원시)", 0],
                ["총 레코드(중복 제거 후)", 0],
            ], columns=["항목","값"]).to_excel(xw, sheet_name="summary", index=False)
        print("\n⚠️ 데이터가 없습니다(필터/권한/기간 확인).")
        print(f"생성: {out_xlsx}")
        return

    # 병합 및 전역 중복 제거
    full = pd.concat(month_frames, ignore_index=True)
    before_raw = len(full)
    for col in ("trprId", "trprDegr"):
        if col not in full.columns:
            full[col] = ""
    full["__key"] = full["trprId"].fillna("") + "|" + full["trprDegr"].fillna("")
    full = full.drop_duplicates(subset="__key").drop(columns="__key")
    after_dedup = len(full)

    # 컬럼 한글화 (60 전용 지표 포함)
    rename = {
        "title": "title(제목)", "subTitle": "subTitle(부 제목)",
        "titleLink": "titleLink(제목 링크)", "subTitleLink": "subTitleLink(부 제목 링크)",
        "titleIcon": "titleIcon(제목 아이콘)", "traStartDate": "traStartDate(훈련시작일자)",
        "traEndDate": "traEndDate(훈련종료일자)", "address": "address(주소)", "telNo": "telNo(전화번호)",
        "instCd": "instCd(훈련기관 코드)", "trainstCstId": "trainstCstId(훈련기관ID)",
        "trainTarget": "trainTarget(훈련대상)", "trainTargetCd": "trainTargetCd(훈련구분)",
        "trngAreaCd": "trngAreaCd(지역코드 중분류)", "trprId": "trprId(훈련과정ID)",
        "trprDegr": "trprDegr(훈련과정 순차)", "ncsCd": "ncsCd(NCS 코드)", "yardMan": "yardMan(정원)",
        "courseMan": "courseMan(수강비)", "realMan": "realMan(실제훈련비)", "regCourseMan": "regCourseMan(수강신청 인원)",
        "grade": "grade(등급)", "contents": "contents(컨텐츠)", "certificate": "certificate(자격증)",
        "stdgScor": "stdgScor(만족도 점수)", "eiEmplCnt3": "eiEmplCnt3(고용보험3개월 취업인원 수)",
        "eiEmplRate3": "eiEmplRate3(3개월 취업률)", "eiEmplRate6": "eiEmplRate6(6개월 취업률)",
        "eiEmplCnt3Gt10": "eiEmplCnt3Gt10(3개월 취업누적 10인 이하 여부)",
    }
    full = full.rename(columns=rename)

    # ✅ 훈련시작일자 기준 오름차순 정렬
    full = full.sort_values(by="traStartDate(훈련시작일자)").reset_index(drop=True)

    # 저장 (xlsxwriter → 없으면 openpyxl 폴백)
    try:
        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xw:
            full.to_excel(xw, sheet_name="data", index=False)
            pd.DataFrame([
                ["API", "HRDPOA60_1 (국민내일배움카드)"],
                ["기간 From", START_DATE.isoformat()],
                ["기간 To", END_DATE.isoformat()],
                ["NCS 1차", "20 (정보통신)"],
                ["pageSize", PAGE_SIZE],
                ["샤딩", "월 → 1000 초과 시 주간"],
                ["총 레코드(원시)", before_raw],
                ["총 레코드(중복 제거 후)", after_dedup],
            ], columns=["항목","값"]).to_excel(xw, sheet_name="summary", index=False)
    except ModuleNotFoundError:
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
            full.to_excel(xw, sheet_name="data", index=False)
            pd.DataFrame([
                ["API", "HRDPOA60_1 (국민내일배움카드)"],
                ["기간 From", START_DATE.isoformat()],
                ["기간 To", END_DATE.isoformat()],
                ["NCS 1차", "20 (정보통신)"],
                ["pageSize", PAGE_SIZE],
                ["샤딩", "월 → 1000 초과 시 주간"],
                ["총 레코드(원시)", before_raw],
                ["총 레코드(중복 제거 후)", after_dedup],
            ], columns=["항목","값"]).to_excel(xw, sheet_name="summary", index=False)

    print(f"\n✅ 저장 완료: {out_xlsx}")
    print(f"총 레코드 수(중복 제거 후): {after_dedup:,d}  (원시: {before_raw:,d})")

if __name__ == "__main__":
    main()
