#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///

"""
KIS 5분봉 전일고가 돌파 스크리너 (7 조건).

조건 (AND):
  1) 현재가가 전일 고가 돌파 (장중 어느 5분봉에서든 high > prev_day_high)
  2) 돌파봉 거래량 >= 직전 3개 5분봉 평균 * 1.5
  3) 돌파봉 종가 > 전일 고가
  4) 돌파봉 윗꼬리 비율 <= 30%
  5) 돌파 직후 다음 5분봉 중 1개 이상이 low >= 전일 고가 (지지)
  6) 당일 거래대금 >= 300억 (500억 이상이면 conds.value_500eok=true 별도 표기)
  7) 장 초반 급등 후 밀리는 형태 제외: 종가 >= 전일고가 * 0.99

평가일 (eval_date): 인자로 지정 (기본 20260416 = 최근 완료 거래일).
"어제 종가를 현재가로" 해석: eval_date = 마지막 완료 거래일, prev = 그 직전 거래일.

Usage:
  uv run screen_breakout.py                              # 기본: min 3000억, 20260416
  uv run screen_breakout.py MIN_CAP                      # 최소 시총
  uv run screen_breakout.py MIN_CAP MAX_CAP              # 시총 [MIN,MAX] 범위 (MAX=0이면 무제한)
  uv run screen_breakout.py MIN_CAP MAX_CAP N            # + max_stocks
  uv run screen_breakout.py MIN_CAP MAX_CAP 0 YYYYMMDD   # + eval_date
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from api_client import KISSession  # noqa: E402
from screen_golden_cross import (  # noqa: E402
    RATE_LIMIT_SLEEP,
    _http_get,
    fetch_market_cap_universe,
)

DAILY_API = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
DAILY_TR = "FHKST03010100"
INTRADAY_API = "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
INTRADAY_TR = "FHKST03010230"


def fetch_daily_pair(
    session: KISSession, code: str, eval_date: datetime
) -> tuple[dict | None, dict | None]:
    """eval_date 이전 최근 2거래일 일봉. 반환: (prev_day, eval_day)."""
    start = eval_date - timedelta(days=10)
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
        "FID_INPUT_DATE_2": eval_date.strftime("%Y%m%d"),
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "0",
    }
    body, _ = _http_get(session, DAILY_API, DAILY_TR, params)
    if body.get("rt_cd") != "0":
        return None, None
    bars: list[dict] = []
    for row in body.get("output2") or []:
        d = row.get("stck_bsop_date", "")
        try:
            o = float(row.get("stck_oprc", "0"))
            h = float(row.get("stck_hgpr", "0"))
            l_ = float(row.get("stck_lwpr", "0"))
            c = float(row.get("stck_clpr", "0"))
            v = float(row.get("acml_vol", "0"))
            val = float(row.get("acml_tr_pbmn", "0"))
        except (TypeError, ValueError):
            continue
        if d and c > 0 and v > 0:
            bars.append(
                {"date": d, "open": o, "high": h, "low": l_, "close": c, "vol": v, "val": val}
            )
    bars.sort(key=lambda b: b["date"])
    # eval_date의 정확히 그 날짜를 찾고, 그 직전을 prev로 선택
    eval_str = eval_date.strftime("%Y%m%d")
    eval_idx = None
    for i, b in enumerate(bars):
        if b["date"] == eval_str:
            eval_idx = i
            break
    if eval_idx is None or eval_idx == 0:
        return None, None
    return bars[eval_idx - 1], bars[eval_idx]


def fetch_5min_bars(
    session: KISSession, code: str, date_str: str, end_hour: str
) -> list[dict]:
    """지정일 장중 1분봉 120개를 가져와 5분봉으로 집계. 오름차순 반환."""
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": date_str,
        "FID_INPUT_HOUR_1": end_hour,
        "FID_PW_DATA_INCU_YN": "N",
        "FID_FAKE_TICK_INCU_YN": "N",
    }
    body, _ = _http_get(session, INTRADAY_API, INTRADAY_TR, params)
    if body.get("rt_cd") != "0":
        return []

    raw: list[dict] = []
    for row in body.get("output2") or []:
        t = row.get("stck_cntg_hour", "")
        if not t or len(t) < 6:
            continue
        if t == "153000":  # 장 마감 요약 바 제외
            continue
        try:
            o = float(row.get("stck_oprc", "0"))
            h = float(row.get("stck_hgpr", "0"))
            l_ = float(row.get("stck_lwpr", "0"))
            c = float(row.get("stck_prpr", "0"))
            v = float(row.get("cntg_vol", "0"))
        except (TypeError, ValueError):
            continue
        if c > 0:
            raw.append({"time": t, "open": o, "high": h, "low": l_, "close": c, "vol": v})

    raw.sort(key=lambda b: b["time"])

    # 1분봉 → 5분봉 집계 (시작 시각 기준: HH(MM//5*5))
    slots: dict[str, dict] = {}
    for b in raw:
        t = b["time"]
        hh = int(t[0:2])
        mm = int(t[2:4])
        slot_m = (mm // 5) * 5
        key = f"{hh:02d}{slot_m:02d}"
        if key not in slots:
            slots[key] = {
                "time": key,
                "open": b["open"],
                "high": b["high"],
                "low": b["low"],
                "close": b["close"],
                "vol": b["vol"],
            }
        else:
            g = slots[key]
            g["high"] = max(g["high"], b["high"])
            g["low"] = min(g["low"], b["low"])
            g["close"] = b["close"]  # raw 오름차순이므로 마지막 close 유지
            g["vol"] += b["vol"]

    return sorted(slots.values(), key=lambda x: x["time"])


def evaluate_breakout(
    bars_5m: list[dict], prev_day_high: float, eval_day: dict
) -> dict | None:
    if not bars_5m or not eval_day or prev_day_high <= 0:
        return None

    # 돌파봉 = high가 처음으로 prev_day_high 초과하는 5분봉
    breakout_idx = None
    for i, b in enumerate(bars_5m):
        if b["high"] > prev_day_high:
            breakout_idx = i
            break

    if breakout_idx is None:
        return {
            "pass": False,
            "conds": {
                "breakout": False,
                "volume_surge": False,
                "close_above": False,
                "upper_shadow_ok": False,
                "support_after": False,
                "value_300eok": False,
                "no_early_fade": False,
            },
            "reason": "전일 고가 돌파 없음",
        }

    bo = bars_5m[breakout_idx]
    cond1 = True

    # ② 거래량 서지
    if breakout_idx >= 3:
        prev3 = bars_5m[breakout_idx - 3 : breakout_idx]
        prev3_avg = sum(b["vol"] for b in prev3) / 3
        cond2 = prev3_avg > 0 and bo["vol"] >= prev3_avg * 1.5
    else:
        prev3_avg = None
        cond2 = False

    # ③ 돌파봉 종가 > 전일 고가
    cond3 = bo["close"] > prev_day_high

    # ④ 윗꼬리 비율
    max_oc = max(bo["open"], bo["close"])
    upper_shadow = bo["high"] - max_oc
    total_range = bo["high"] - bo["low"]
    if total_range > 0:
        upper_ratio = upper_shadow / total_range
        cond4 = upper_ratio <= 0.30
    else:
        upper_ratio = 0.0
        cond4 = True

    # ⑤ 돌파 직후 N봉 중 1개 이상이 low >= prev_day_high
    next_bars = bars_5m[breakout_idx + 1 : breakout_idx + 6]
    support_count = sum(1 for b in next_bars if b["low"] >= prev_day_high)
    cond5 = support_count >= 1

    # ⑥ 거래대금 300억
    val = eval_day["val"]
    cond6 = val >= 30_000_000_000
    strong_500 = val >= 50_000_000_000

    # ⑦ 장 초반 급등 후 밀림 제외: 종가 >= 전일고가 * 0.99
    cond7 = eval_day["close"] >= prev_day_high * 0.99

    all_pass = all([cond1, cond2, cond3, cond4, cond5, cond6, cond7])

    return {
        "pass": all_pass,
        "conds": {
            "breakout": cond1,
            "volume_surge": cond2,
            "close_above": cond3,
            "upper_shadow_ok": cond4,
            "support_after": cond5,
            "value_300eok": cond6,
            "no_early_fade": cond7,
        },
        "value_500eok": strong_500,
        "details": {
            "breakout_time": bo["time"],
            "breakout_ohlc": [bo["open"], bo["high"], bo["low"], bo["close"]],
            "breakout_vol": int(bo["vol"]),
            "prev3_avg_vol": int(prev3_avg) if prev3_avg else None,
            "prev_day_high": prev_day_high,
            "upper_shadow_ratio": round(upper_ratio, 3),
            "support_bars_above_ph": support_count,
            "eval_day_close": eval_day["close"],
            "eval_day_value_eok": round(val / 100_000_000, 1),
        },
    }


def main():
    args = sys.argv[1:]
    min_cap = int(args[0]) if len(args) > 0 else 3000
    max_cap = int(args[1]) if len(args) > 1 else 0
    max_stocks = int(args[2]) if len(args) > 2 else 0
    eval_date_str = args[3] if len(args) > 3 else "20260416"

    try:
        eval_date = datetime.strptime(eval_date_str, "%Y%m%d")
    except ValueError:
        print(json.dumps({"error": f"invalid eval_date: {eval_date_str}"}, ensure_ascii=False))
        sys.exit(1)

    session = KISSession()
    print(
        f"[info] mode={session.mode} min_cap={min_cap}억 eval_date={eval_date_str}"
        + (f" max_stocks={max_stocks}" if max_stocks else ""),
        file=sys.stderr,
    )

    print("[step 1/3] 가격 버킷별 시가총액 상위 수집", file=sys.stderr)
    all_stocks = fetch_market_cap_universe(session)
    if not all_stocks:
        print(json.dumps({"error": "시가총액 조회 실패"}, ensure_ascii=False))
        sys.exit(1)

    def _to_int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    filtered: list[dict] = []
    for row in all_stocks:
        cap = _to_int(row.get("stck_avls"))
        if cap >= min_cap and (max_cap == 0 or cap <= max_cap):
            filtered.append(
                {
                    "code": row.get("mksc_shrn_iscd", ""),
                    "name": row.get("hts_kor_isnm", ""),
                    "market_cap_eok": cap,
                }
            )
    if max_stocks:
        filtered = filtered[:max_stocks]

    cap_desc = f"{min_cap}억" + (f"~{max_cap}억" if max_cap else "+")
    print(
        f"[step 2/3] 필터: {len(filtered)}종목 (시총 {cap_desc})",
        file=sys.stderr,
    )

    if not filtered:
        print(json.dumps({"match_count": 0, "matches": []}, ensure_ascii=False, indent=2))
        return

    est = len(filtered) * (RATE_LIMIT_SLEEP * 3)
    print(f"[step 3/3] 일봉 + 분봉 조회 및 평가 (~{est:.0f}초 예상)", file=sys.stderr)

    matches: list[dict] = []
    partial: list[dict] = []
    pass_cond1: list[dict] = []
    pass_cond1_and_2: list[dict] = []
    cond_tally = {
        "breakout": 0,
        "volume_surge": 0,
        "close_above": 0,
        "upper_shadow_ok": 0,
        "support_after": 0,
        "value_300eok": 0,
        "no_early_fade": 0,
    }
    evaluated = 0
    strong_500_count = 0

    for i, stock in enumerate(filtered, 1):
        prev_day, eval_day = fetch_daily_pair(session, stock["code"], eval_date)
        time.sleep(RATE_LIMIT_SLEEP)
        if prev_day is None or eval_day is None:
            continue

        bars_5m = fetch_5min_bars(session, stock["code"], eval_date_str, "113000")
        time.sleep(RATE_LIMIT_SLEEP)
        if not bars_5m:
            continue

        ev = evaluate_breakout(bars_5m, prev_day["high"], eval_day)
        if ev is None:
            continue
        evaluated += 1
        for k, v in ev["conds"].items():
            if v:
                cond_tally[k] += 1
        if ev.get("value_500eok"):
            strong_500_count += 1

        total_pass = sum(ev["conds"].values())
        record = {
            **stock,
            "prev_day_high": prev_day["high"],
            **(ev.get("details") or {}),
            "conds": ev["conds"],
            "conds_passed": total_pass,
            "value_500eok": ev.get("value_500eok", False),
        }

        if ev["conds"]["breakout"]:
            pass_cond1.append(record)
            if ev["conds"]["volume_surge"]:
                pass_cond1_and_2.append(record)

        if ev["pass"]:
            matches.append(record)
            bt = (ev.get("details") or {}).get("breakout_time", "?")
            print(
                f"  [{i}/{len(filtered)}] MATCH {stock['name']} ({stock['code']}) @ {bt}",
                file=sys.stderr,
            )
        elif total_pass >= 5:
            partial.append(record)

        if i % 20 == 0:
            print(
                f"  [{i}/{len(filtered)}] / 매칭 {len(matches)} / 5+ 부분 {len(partial)}",
                file=sys.stderr,
            )

    matches.sort(key=lambda m: (m["value_500eok"], m["market_cap_eok"]), reverse=True)
    partial.sort(key=lambda m: (m["conds_passed"], m["market_cap_eok"]), reverse=True)
    pass_cond1.sort(key=lambda m: m["market_cap_eok"], reverse=True)
    pass_cond1_and_2.sort(key=lambda m: m["market_cap_eok"], reverse=True)

    stats = {
        k: f"{v}/{evaluated} ({v/evaluated*100:.1f}%)" if evaluated else "0/0"
        for k, v in cond_tally.items()
    }

    print(
        json.dumps(
            {
                "eval_date": eval_date_str,
                "min_market_cap_eok": min_cap,
                "max_market_cap_eok": max_cap,
                "scanned": len(all_stocks),
                "filtered": len(filtered),
                "evaluated": evaluated,
                "condition_pass_rates": stats,
                "match_count": len(matches),
                "strong_value_500eok_count": strong_500_count,
                "matches": matches,
                "cond1_pass_count": len(pass_cond1),
                "cond1_pass": pass_cond1,
                "cond1_and_2_pass_count": len(pass_cond1_and_2),
                "cond1_and_2_pass": pass_cond1_and_2,
                "partial_match_count_5plus": len(partial),
                "top_partial": partial[:10],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
