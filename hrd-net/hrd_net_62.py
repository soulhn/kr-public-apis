import os, math, time, random, datetime as dt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry

#사업주훈련과정_202401_202508_NCS정보통신_82,497건
# ■ 실행/출력
# - 기간: START_DATE~END_DATE, NCS 1차(srchncs1=20, 정보통신)
# - 월 단위 샤딩 수집, 월 페이지가 1000 초과 시 해당 월만 주간 샤딩으로 세분화
# - 멀티스레드(ThreadPoolExecutor)로 월 샤드 병렬 수집 (MAX_WORKERS로 조절)
#   - Sheet 'data'    : 원본/정제 데이터 (중복 제거 후)
#   - Sheet 'summary' : 수집 메타 정보 요약

# ===== 기본 설정 =====
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
PAGE_SIZE = 100
MAX_WORKERS = 5
USER_AGENT = "HRDNET-Collector/1.0"

BASE_URL = "https://hrd.work24.go.kr/jsp/HRDP/HRDPO00/HRDPOA62/HRDPOA62_1.jsp"
BASE_PARAMS = {
    "authKey": AUTH_KEY,
    "returnType": "XML",   # ✅ XML로 안정성 우선
    "outType": "1",
    "pageSize": str(PAGE_SIZE),
    "sort": "ASC",
    "sortCol": "2",
    "srchNcs1": "20",      # ✅ 정보통신
     "crseTracseSe": "C0041N"
}

def month_shards(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    end_m = dt.date(end.year, end.month, 1)
    while cur <= end_m:
        if cur.month == 12: last = dt.date(cur.year, 12, 31)
        else: last = dt.date(cur.year, cur.month + 1, 1) - dt.timedelta(days=1)
        yield max(cur, start), min(last, end)
        cur = (dt.date(cur.year + 1, 1, 1) if cur.month == 12 else dt.date(cur.year, cur.month + 1, 1))

def ymd(d: dt.date): return d.strftime("%Y%m%d")

def new_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.6, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})
    return s

def fetch_xml(session: requests.Session, params: dict, page_num: int) -> BeautifulSoup:
    p = params.copy(); p["pageNum"] = str(page_num)
    r = session.get(BASE_URL, params=p, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.content, "lxml-xml")

def parse_rows_xml(soup: BeautifulSoup):
    out = []
    srch_list = soup.find("srchList")
    if not srch_list:
        return out
    for scn in srch_list.find_all("scn_list"):
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
        })
    return out

def collect_one_shard(idx, shard):
    sdate, edate = shard
    sess = new_session()
    params = BASE_PARAMS.copy()
    params["srchTraStDt"] = ymd(sdate)
    params["srchTraEndDt"] = ymd(edate)

    # 첫 페이지
    soup1 = fetch_xml(sess, params, 1)
    scn_cnt_text = soup1.find("scn_cnt").text if soup1.find("scn_cnt") else "0"
    total_cnt = int(scn_cnt_text) if scn_cnt_text.isdigit() else 0
    total_pages = math.ceil(total_cnt / PAGE_SIZE) if total_cnt else 0
    print(f"[{idx:02d}] {sdate}~{edate} | count={total_cnt}, pages={total_pages}")

    if total_pages == 0:
        return pd.DataFrame(), 0

    recs = parse_rows_xml(soup1)
    for p in range(2, total_pages + 1):
        time.sleep(0.05 + random.random() * 0.1)
        try:
            soup = fetch_xml(sess, params, p)
            recs.extend(parse_rows_xml(soup))
        except requests.RequestException as e:
            print(f"[{idx:02d}] page {p} error: {e}")

    df = pd.DataFrame(recs)
    return df, total_cnt

def main():
    shards = list(month_shards(START_DATE, END_DATE))
    print(f"Total shards: {len(shards)} (month)")

    results = []
    total_cnt_sum = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(collect_one_shard, i, shard): i for i, shard in enumerate(shards, 1)}
        for fut in as_completed(futs):
            i = futs[fut]
            try:
                df, shard_cnt = fut.result()
                results.append(df)
                total_cnt_sum += shard_cnt
            except Exception as e:
                print(f"[{i:02d}] shard failed: {e}")

    if not results or all(len(df)==0 for df in results):
        # 아무 데이터 없어도 summary만 저장해서 흔적 남김
        out_xlsx = f"{dt.datetime.now().strftime('%y%m%d%H%M')}_employer_training_courses.xlsx"
        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xw:
            pd.DataFrame().to_excel(xw, sheet_name="data", index=False)
            meta = pd.DataFrame([
                ["수집 기간 From", START_DATE.isoformat()],
                ["수집 기간 To", END_DATE.isoformat()],
                ["returnType", BASE_PARAMS["returnType"]],
                ["pageSize", PAGE_SIZE],
                ["병렬수(MAX_WORKERS)", MAX_WORKERS],
                ["샤드 수", len(shards)],
                ["원시 레코드 수(병합 전)", 0],
                ["중복 제거 후 총 레코드 수", 0],
                ["샤드별 scn_cnt 합(참고)", total_cnt_sum],
            ], columns=["항목", "값"])
            meta.to_excel(xw, sheet_name="summary", index=False)
        print("\n⚠️ 수집된 데이터가 없습니다. (필터/기간/엔드포인트/권한 확인 필요)")
        print(f"파일 생성: {out_xlsx} (summary만 포함)")
        return

    full = pd.concat(results, ignore_index=True)
    before_raw = len(full)

    # 빈 데이터 대비 컬럼 없을 수 있어 보호코드
    for col in ("trprId", "trprDegr"):
        if col not in full.columns:
            full[col] = ""

    full["__key"] = full["trprId"].fillna("") + "|" + full["trprDegr"].fillna("")
    full = full.drop_duplicates(subset="__key").drop(columns="__key")
    after_dedup = len(full)

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
        "grade": "grade(등급)", "contents": "contents(컨텐츠)", "certificate": "certificate(자격증)"
    }
    full = full.rename(columns=rename)

    out_xlsx = f"{dt.datetime.now().strftime('%y%m%d%H%M')}_employer_training_courses.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xw:
        full.to_excel(xw, sheet_name="data", index=False)
        meta = pd.DataFrame([
            ["수집 기간 From", START_DATE.isoformat()],
            ["수집 기간 To", END_DATE.isoformat()],
            ["returnType", BASE_PARAMS["returnType"]],
            ["pageSize", PAGE_SIZE],
            ["병렬수(MAX_WORKERS)", MAX_WORKERS],
            ["샤드 수", len(shards)],
            ["원시 레코드 수(병합 전)", before_raw],
            ["중복 제거 후 총 레코드 수", after_dedup],
            ["샤드별 scn_cnt 합(참고)", total_cnt_sum],
        ], columns=["항목", "값"])
        meta.to_excel(xw, sheet_name="summary", index=False)

    print(f"\n✅ 저장 완료: {out_xlsx}")
    print(f"총 레코드 수(중복 제거 후): {after_dedup:,d} (원시: {before_raw:,d})")
    print(f"(참고) 샤드별 scn_cnt 합: {total_cnt_sum:,d}")

if __name__ == "__main__":
    main()
