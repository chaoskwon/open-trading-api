#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///

"""
KIS 골든크로스 스크리너.

시가총액 N억 이상 종목 중 단기/장기 이동평균선 골든크로스 발생 종목을 찾는다.
stdlib only (api_client.KISSession 재사용). 민감 값은 출력하지 않는다.

Usage:
  uv run screen_golden_cross.py                    # 기본: 3000억, 20/60
  uv run screen_golden_cross.py 3000 20 60         # min_cap(억) short_ma long_ma
  uv run screen_golden_cross.py 3000 20 60 50      # + max_stocks (상위 N개만 검사)
"""

import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from api_client import KISSession, _convert_tr_id  # noqa: E402

RATE_LIMIT_SLEEP = 0.7  # 모의투자 2 TPS 한도 대응 (safety margin)


def _http_get(
    session: KISSession,
    api_path: str,
    tr_id: str,
    params: dict,
    tr_cont: str = "",
    retries: int = 3,
) -> tuple[dict, str]:
    """KIS GET 호출. (body_json, response_tr_cont) 반환.

    EGW00201 (초당 거래건수 초과) 응답 시 지수 backoff로 자동 재시도.
    """
    real_tr_id = _convert_tr_id(tr_id, session.mode)
    url = f"{session._base_url}{api_path}?{urllib.parse.urlencode(params)}"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {session.token}",
        "appkey": session._appkey,
        "appsecret": session._appsecret,
        "tr_id": real_tr_id,
        "custtype": "P",
        "tr_cont": tr_cont,
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    ctx = ssl.create_default_context()

    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                resp_tr_cont = resp.headers.get("tr_cont", "")
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace") if e.fp else str(e)
            body = {"error": raw[:300], "rt_cd": "-1"}
            resp_tr_cont = ""
        except Exception as e:
            body = {"error": str(e), "rt_cd": "-1"}
            resp_tr_cont = ""

        if body.get("msg_cd") == "EGW00201" and attempt < retries:
            time.sleep(1.0 + attempt * 0.5)
            continue
        return body, resp_tr_cont

    return body, resp_tr_cont


# KIS 시가총액 랭킹 API는 단일 호출에 top 30만 반환하고 tr_cont 연속조회가 작동하지
# 않는다. 가격 범위(fid_input_price_1/2)로 슬라이싱해 여러 번 호출하면 각 슬라이스별
# top 30이 반환되므로, 가격 버킷을 나눠 더 넓게 커버한다. 같은 종목이 겹치지 않도록
# 버킷은 연속되고, 결과는 종목코드로 de-dupe 한다.
PRICE_BUCKETS: list[tuple[str, str]] = [
    ("1000", "2000"),
    ("2000", "3000"),
    ("3000", "5000"),
    ("5000", "10000"),
    ("10000", "20000"),
    ("20000", "50000"),
    ("50000", "100000"),
    ("100000", "300000"),
    ("300000", "1000000"),
    ("1000000", "10000000"),
]


def fetch_market_cap_universe(session: KISSession) -> list[dict]:
    """가격 버킷을 돌며 시가총액 상위 30종목씩 수집 후 de-dupe."""
    rows: list[dict] = []
    seen: set[str] = set()
    for lo, hi in PRICE_BUCKETS:
        params = {
            "fid_input_price_2": hi,
            "fid_cond_mrkt_div_code": "J",
            "fid_cond_scr_div_code": "20174",
            "fid_div_cls_code": "0",
            "fid_input_iscd": "0000",
            "fid_trgt_cls_code": "0",
            "fid_trgt_exls_cls_code": "0",
            "fid_input_price_1": lo,
            "fid_vol_cnt": "",
        }
        body, _ = _http_get(
            session,
            "/uapi/domestic-stock/v1/ranking/market-cap",
            "FHPST01740000",
            params,
        )
        if body.get("rt_cd") != "0":
            msg = body.get("msg1") or body.get("error") or "unknown"
            print(f"[warn] 가격 {lo}~{hi} 실패: {msg}", file=sys.stderr)
            continue
        output = body.get("output") or []
        new_count = 0
        for row in output:
            code = row.get("mksc_shrn_iscd", "")
            if code and code not in seen:
                seen.add(code)
                rows.append(row)
                new_count += 1
        print(
            f"[info] 가격 {lo:>8}~{hi:<8}: +{new_count} / total {len(rows)}",
            file=sys.stderr,
        )
        time.sleep(RATE_LIMIT_SLEEP)
    return rows


def fetch_daily_closes(
    session: KISSession, code: str, min_bars: int
) -> list[tuple[str, float]]:
    """종목 일봉 종가 리스트 (오래된 날짜 → 최신순).

    KIS 일봉 API는 한 번에 최대 100바만 반환하므로, min_bars 확보 시까지
    종료 일자를 뒤로 밀면서 여러 청크로 나눠 호출한다.
    """
    closes_map: dict[str, float] = {}
    end_date = datetime.now()
    # 100 거래일 ≈ 145 달력일. 안전마진 포함 150일 윈도우.
    window_days = 150
    max_chunks = (min_bars // 90) + 2

    for chunk_idx in range(max_chunks):
        start_date = end_date - timedelta(days=window_days)
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start_date.strftime("%Y%m%d"),
            "FID_INPUT_DATE_2": end_date.strftime("%Y%m%d"),
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }
        body, _ = _http_get(
            session,
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            "FHKST03010100",
            params,
        )
        if body.get("rt_cd") != "0":
            break

        chunk = body.get("output2") or []
        added = 0
        for row in chunk:
            date = row.get("stck_bsop_date", "")
            try:
                close = float(row.get("stck_clpr", "0"))
            except (TypeError, ValueError):
                continue
            if date and close > 0 and date not in closes_map:
                closes_map[date] = close
                added += 1

        if len(closes_map) >= min_bars:
            break
        if added == 0:
            break

        # 다음 청크는 현재까지의 가장 오래된 바로 직전까지.
        oldest_str = min(closes_map.keys())
        try:
            oldest_dt = datetime.strptime(oldest_str, "%Y%m%d")
        except ValueError:
            break
        end_date = oldest_dt - timedelta(days=1)

        if chunk_idx < max_chunks - 1:
            time.sleep(RATE_LIMIT_SLEEP)

    return sorted(closes_map.items(), key=lambda x: x[0])


def check_golden_cross(
    closes: list[tuple[str, float]], short_n: int, long_n: int
) -> dict | None:
    n = len(closes)
    if n < long_n + 1:
        return None

    def ma(end_idx: int, window: int) -> float:
        start = end_idx - window + 1
        return sum(c for _, c in closes[start : end_idx + 1]) / window

    today_i = n - 1
    yday_i = n - 2
    ts = ma(today_i, short_n)
    tl = ma(today_i, long_n)
    ys = ma(yday_i, short_n)
    yl = ma(yday_i, long_n)

    if ys <= yl and ts > tl:
        return {
            "signal_date": closes[today_i][0],
            "close": closes[today_i][1],
            f"ma{short_n}": round(ts, 2),
            f"ma{long_n}": round(tl, 2),
            "gap_pct": round((ts - tl) / tl * 100, 3),
        }
    return None


def main():
    args = sys.argv[1:]
    min_cap = int(args[0]) if len(args) > 0 else 3000
    short_n = int(args[1]) if len(args) > 1 else 20
    long_n = int(args[2]) if len(args) > 2 else 60
    max_stocks = int(args[3]) if len(args) > 3 else 0  # 0 = 무제한

    if short_n >= long_n:
        print(json.dumps({"error": "short_ma는 long_ma보다 작아야 합니다"}, ensure_ascii=False))
        sys.exit(1)

    session = KISSession()
    print(
        f"[info] mode={session.mode} min_cap={min_cap}억 MA={short_n}/{long_n}"
        + (f" max_stocks={max_stocks}" if max_stocks else ""),
        file=sys.stderr,
    )

    print("[step 1/3] 가격 버킷별 시가총액 상위 수집", file=sys.stderr)
    all_stocks = fetch_market_cap_universe(session)
    if not all_stocks:
        print(json.dumps({"error": "시가총액 상위 조회 실패"}, ensure_ascii=False))
        sys.exit(1)

    # 단위 디버그: 첫 행의 stck_avls 값을 그대로 노출 (민감 정보 아님)
    sample = all_stocks[0]
    print(
        f"[debug] sample: {sample.get('hts_kor_isnm')} "
        f"stck_avls={sample.get('stck_avls')} (억원 단위 가정)",
        file=sys.stderr,
    )

    def _to_int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    filtered: list[dict] = []
    for row in all_stocks:
        cap = _to_int(row.get("stck_avls"))
        if cap >= min_cap:
            filtered.append(
                {
                    "code": row.get("mksc_shrn_iscd", ""),
                    "name": row.get("hts_kor_isnm", ""),
                    "price": _to_int(row.get("stck_prpr")),
                    "market_cap_eok": cap,
                }
            )

    if max_stocks and len(filtered) > max_stocks:
        filtered = filtered[:max_stocks]

    print(
        f"[step 2/3] 필터: {len(filtered)}종목 (전체 {len(all_stocks)}개 중 시총 >= {min_cap}억)",
        file=sys.stderr,
    )

    result_base = {
        "min_market_cap_eok": min_cap,
        "short_ma": short_n,
        "long_ma": long_n,
        "scanned": len(all_stocks),
        "filtered": len(filtered),
    }

    if not filtered:
        print(json.dumps({**result_base, "match_count": 0, "matches": []}, ensure_ascii=False, indent=2))
        return

    est = len(filtered) * (RATE_LIMIT_SLEEP + 0.3)
    print(
        f"[step 3/3] 차트 조회 + 골든크로스 판정 (~{est:.0f}초 예상)",
        file=sys.stderr,
    )

    matches: list[dict] = []
    for i, stock in enumerate(filtered, 1):
        closes = fetch_daily_closes(session, stock["code"], min_bars=long_n + 5)
        if closes:
            cross = check_golden_cross(closes, short_n, long_n)
            if cross:
                matches.append({**stock, **cross})
                print(
                    f"  [{i}/{len(filtered)}] MATCH {stock['name']} ({stock['code']})",
                    file=sys.stderr,
                )
        if i % 20 == 0:
            print(
                f"  [{i}/{len(filtered)}] 진행중 / 누적 매칭 {len(matches)}건",
                file=sys.stderr,
            )
        time.sleep(RATE_LIMIT_SLEEP)

    matches.sort(key=lambda m: m["market_cap_eok"], reverse=True)

    print(
        json.dumps(
            {**result_base, "match_count": len(matches), "matches": matches},
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
