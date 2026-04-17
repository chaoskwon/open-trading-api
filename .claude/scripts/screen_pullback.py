#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///

"""
KIS 눌림목 스크리너 (6 조건).

조건 (AND):
  1) 20일선 우상향:   오늘 MA20 > 5거래일 전 MA20
  2) 주가가 20일선 근처:  |종가 - MA20| / MA20 <= 3%
  3) BB 하단 근접:   종가 <= (MA20 - 2*std20) * 1.05
  4) RSI 40~50 반등: 40 <= RSI(14) <= 50  AND  오늘 RSI > 어제 RSI
  5) 하락구간 거래량 감소: 최근 5일 하락일 평균거래량 < 20일 평균 * 0.8
  6) 양봉 또는 꼬리 지지: 종가>시가 OR 아래꼬리 >= 몸통*2

Usage:
  uv run screen_pullback.py                      # 기본: min 3000억
  uv run screen_pullback.py MIN_CAP              # 최소 시총(억)
  uv run screen_pullback.py MIN_CAP MAX_CAP      # 시총 [MIN,MAX] 범위 (MAX=0 무제한)
  uv run screen_pullback.py MIN_CAP MAX_CAP N    # + max_stocks
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


def fetch_daily_ohlcv(session: KISSession, code: str, min_bars: int = 40) -> list[dict]:
    """종목의 일봉 OHLCV 리스트 (오래된 날짜 → 최신순). 단일 호출로 최대 100바."""
    today = datetime.now()
    start = today - timedelta(days=int(min_bars * 1.8) + 15)
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
        "FID_INPUT_DATE_2": today.strftime("%Y%m%d"),
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
        return []

    bars: list[dict] = []
    for row in body.get("output2") or []:
        d = row.get("stck_bsop_date", "")
        try:
            o = float(row.get("stck_oprc", "0"))
            h = float(row.get("stck_hgpr", "0"))
            l_ = float(row.get("stck_lwpr", "0"))
            c = float(row.get("stck_clpr", "0"))
            v = float(row.get("acml_vol", "0"))
        except (TypeError, ValueError):
            continue
        if d and c > 0 and o > 0 and v > 0:
            bars.append({"date": d, "open": o, "high": h, "low": l_, "close": c, "vol": v})
    bars.sort(key=lambda b: b["date"])
    return bars


def compute_rsi(closes: list[float], period: int = 14) -> list[float | None]:
    """Wilder's RSI. 반환 길이 = closes 길이. 앞 period개 위치는 None."""
    n = len(closes)
    if n < period + 1:
        return [None] * n

    gains = [0.0]
    losses = [0.0]
    for i in range(1, n):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))

    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period

    rsis: list[float | None] = [None] * period
    rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
    rsis.append(100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + rs))

    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
        rsis.append(100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + rs))

    return rsis


def evaluate(bars: list[dict]) -> dict | None:
    """마지막 바 기준 6개 조건 평가. 데이터 부족 시 None."""
    n = len(bars)
    if n < 25:
        return None

    idx = n - 1
    closes = [b["close"] for b in bars]
    vols = [b["vol"] for b in bars]

    ma20_today = sum(closes[idx - 19 : idx + 1]) / 20
    ma20_5d_ago = sum(closes[idx - 24 : idx - 4]) / 20
    cond1 = ma20_today > ma20_5d_ago

    c_today = closes[idx]
    dist_pct = abs(c_today - ma20_today) / ma20_today * 100
    cond2 = dist_pct <= 3.0

    window = closes[idx - 19 : idx + 1]
    mean = sum(window) / 20
    var = sum((c - mean) ** 2 for c in window) / 20
    std = var ** 0.5
    bb_lower = mean - 2 * std
    cond3 = c_today <= bb_lower * 1.05

    rsis = compute_rsi(closes, 14)
    rsi_today = rsis[idx]
    rsi_yday = rsis[idx - 1]
    if rsi_today is None or rsi_yday is None:
        return None
    cond4 = (40.0 <= rsi_today <= 50.0) and (rsi_today > rsi_yday)

    vol20_avg = sum(vols[idx - 19 : idx + 1]) / 20
    down_vols = [
        vols[i] for i in range(idx - 4, idx + 1) if i > 0 and closes[i] < closes[i - 1]
    ]
    if down_vols:
        down_avg = sum(down_vols) / len(down_vols)
        cond5 = down_avg < vol20_avg * 0.8
    else:
        down_avg = None
        cond5 = False

    o = bars[idx]["open"]
    h = bars[idx]["high"]
    l_ = bars[idx]["low"]
    c = bars[idx]["close"]
    bullish = c > o
    body = abs(c - o)
    lower_shadow = min(o, c) - l_
    has_tail_support = body > 0 and lower_shadow >= body * 2
    cond6 = bullish or has_tail_support

    all_pass = all([cond1, cond2, cond3, cond4, cond5, cond6])

    return {
        "pass": all_pass,
        "conds": {
            "ma20_uptrend": cond1,
            "near_ma20": cond2,
            "bb_lower": cond3,
            "rsi_rebound": cond4,
            "volume_decline": cond5,
            "bullish_or_tail": cond6,
        },
        "values": {
            "date": bars[idx]["date"],
            "close": c,
            "open": o,
            "high": h,
            "low": l_,
            "ma20": round(ma20_today, 2),
            "ma20_5d_ago": round(ma20_5d_ago, 2),
            "dist_from_ma20_pct": round((c_today - ma20_today) / ma20_today * 100, 2),
            "bb_lower": round(bb_lower, 2),
            "rsi": round(rsi_today, 2),
            "rsi_prev": round(rsi_yday, 2),
            "vol_today": int(vols[idx]),
            "vol20_avg": int(vol20_avg),
            "down_vol_avg": int(down_avg) if down_avg is not None else None,
        },
    }


def main():
    args = sys.argv[1:]
    min_cap = int(args[0]) if len(args) > 0 else 3000
    max_cap = int(args[1]) if len(args) > 1 else 0
    max_stocks = int(args[2]) if len(args) > 2 else 0

    session = KISSession()
    cap_desc = f"{min_cap}억" + (f"~{max_cap}억" if max_cap else "+")
    print(
        f"[info] mode={session.mode} cap={cap_desc}"
        + (f" max_stocks={max_stocks}" if max_stocks else ""),
        file=sys.stderr,
    )

    print("[step 1/3] 가격 버킷별 시가총액 상위 수집", file=sys.stderr)
    all_stocks = fetch_market_cap_universe(session)
    if not all_stocks:
        print(json.dumps({"error": "시가총액 상위 조회 실패"}, ensure_ascii=False))
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
                    "price": _to_int(row.get("stck_prpr")),
                    "market_cap_eok": cap,
                }
            )

    if max_stocks and len(filtered) > max_stocks:
        filtered = filtered[:max_stocks]

    print(
        f"[step 2/3] 필터: {len(filtered)}종목 (전체 {len(all_stocks)}개 중 시총 {cap_desc})",
        file=sys.stderr,
    )

    result_base = {
        "min_market_cap_eok": min_cap,
        "max_market_cap_eok": max_cap,
        "scanned": len(all_stocks),
        "filtered": len(filtered),
    }

    if not filtered:
        print(json.dumps({**result_base, "match_count": 0, "matches": []}, ensure_ascii=False, indent=2))
        return

    est = len(filtered) * (RATE_LIMIT_SLEEP + 0.2)
    print(f"[step 3/3] 일봉 조회 + 조건 평가 (~{est:.0f}초 예상)", file=sys.stderr)

    matches: list[dict] = []
    partial_matches: list[dict] = []
    cond_pass_count = {
        "ma20_uptrend": 0,
        "near_ma20": 0,
        "bb_lower": 0,
        "rsi_rebound": 0,
        "volume_decline": 0,
        "bullish_or_tail": 0,
    }
    eval_count = 0
    for i, stock in enumerate(filtered, 1):
        bars = fetch_daily_ohlcv(session, stock["code"], min_bars=35)
        if bars:
            ev = evaluate(bars)
            if ev is not None:
                eval_count += 1
                for k, v in ev["conds"].items():
                    if v:
                        cond_pass_count[k] += 1
                total_pass = sum(ev["conds"].values())
                if ev["pass"]:
                    matches.append({**stock, **ev["values"], "conds": ev["conds"]})
                    print(
                        f"  [{i}/{len(filtered)}] MATCH {stock['name']} ({stock['code']})",
                        file=sys.stderr,
                    )
                elif total_pass >= 3:
                    partial_matches.append(
                        {
                            **stock,
                            "conds_passed": total_pass,
                            **ev["values"],
                            "conds": ev["conds"],
                        }
                    )
        if i % 20 == 0:
            print(
                f"  [{i}/{len(filtered)}] 진행중 / 매칭 {len(matches)} / 3+ 부분매칭 {len(partial_matches)}",
                file=sys.stderr,
            )
        time.sleep(RATE_LIMIT_SLEEP)

    matches.sort(key=lambda m: m["market_cap_eok"], reverse=True)
    partial_matches.sort(
        key=lambda m: (m["conds_passed"], m["market_cap_eok"]), reverse=True
    )

    stats = {
        k: f"{v}/{eval_count} ({v/eval_count*100:.1f}%)" if eval_count else "0/0"
        for k, v in cond_pass_count.items()
    }

    print(
        json.dumps(
            {
                **result_base,
                "evaluated": eval_count,
                "condition_pass_rates": stats,
                "match_count": len(matches),
                "matches": matches,
                "partial_match_count_3plus": len(partial_matches),
                "top_partial_matches": partial_matches[:15],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
