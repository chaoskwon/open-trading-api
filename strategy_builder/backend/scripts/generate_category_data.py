#!/usr/bin/env python3
"""
KIS 업종/테마/종목 마스터 파일 다운로드 및 JSON 변환.

다운로드 대상:
  - idxcode.mst      : 업종(지수) 코드+이름
  - theme_code.mst   : 테마 코드+이름 + 테마별 종목코드
  - kospi_code.mst   : 코스피 종목 마스터 (업종/시가총액 포함)
  - kosdaq_code.mst  : 코스닥 종목 마스터 (업종/시가총액 포함)

출력:
  backend/data/sectors.json        : [{code, name}]
  backend/data/themes.json         : [{code, name, count}]
  backend/data/theme_stocks.json   : {theme_code: [stock_codes]}
  backend/data/stocks_master.json  : {stock_code: {name, market, sector_code, market_cap_eok}}

Usage:
  uv run backend/scripts/generate_category_data.py
"""

import io
import json
import ssl
import sys
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BACKEND_DIR / "data"
TMP_DIR = BACKEND_DIR / "data" / "_tmp"

SECTOR_URL = "https://new.real.download.dws.co.kr/common/master/idxcode.mst.zip"
THEME_URL = "https://new.real.download.dws.co.kr/common/master/theme_code.mst.zip"
KOSPI_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
KOSDAQ_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"


def _download_and_extract(url: str, dest_dir: Path) -> Path:
    """Zip 다운로드 후 압축해제. 추출된 .mst 파일 경로 반환."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dest_dir / "download.zip"

    # KIS 서버는 자가서명 인증서를 쓸 수 있어 ssl context 완화
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(url, context=ctx, timeout=30) as resp:
        zip_path.write_bytes(resp.read())

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest_dir)

    mst_files = list(dest_dir.glob("*.mst"))
    if not mst_files:
        raise RuntimeError(f"No .mst file extracted from {url}")
    return mst_files[0]


def parse_sectors(mst_path: Path) -> list[dict]:
    """업종 마스터 파싱. 행 포맷: [업종코드 4자 + 업종명 가변]."""
    sectors: list[dict] = []
    seen: set[str] = set()
    with open(mst_path, encoding="cp949") as f:
        for row in f:
            row = row.rstrip("\r\n")
            if len(row) < 5:
                continue
            code = row[1:5].strip()
            name = row[5:].strip()
            if not code or code in seen or not name:
                continue
            seen.add(code)
            sectors.append({"code": code, "name": name})
    return sectors


def parse_themes(mst_path: Path) -> tuple[list[dict], dict[str, list[str]]]:
    """
    테마 마스터 파싱. 각 행 = (테마코드 3자 + 테마명 + 종목코드 10자 우측정렬).

    반환: (themes_list, theme_code → [stock_codes])
      - themes_list: [{code, name, count}]
      - theme_stocks: {code: [stock_codes]}
    """
    theme_stocks: dict[str, list[str]] = {}
    theme_names: dict[str, str] = {}

    with open(mst_path, encoding="cp949") as f:
        for row in f:
            row = row.rstrip("\r\n")
            if len(row) < 14:
                continue
            tcode = row[0:3].strip()
            if not tcode:
                continue
            stock_code = row[-10:].strip()
            tname = row[3:-10].strip()
            if not tname:
                continue

            theme_names.setdefault(tcode, tname)
            if stock_code:
                theme_stocks.setdefault(tcode, []).append(stock_code)

    themes = [
        {"code": code, "name": theme_names[code], "count": len(theme_stocks.get(code, []))}
        for code in sorted(theme_names.keys())
    ]
    return themes, theme_stocks


KOSPI_WIDTHS = [
    2, 1, 4, 4, 4, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 9, 5, 5, 1, 1, 1, 2, 1, 1,
    1, 2, 2, 2, 3, 1, 3, 12, 12, 8,
    15, 21, 2, 7, 1, 1, 1, 1, 1, 9,
    9, 9, 5, 9, 8, 9, 3, 1, 1, 1,
]
KOSPI_COLUMNS = [
    "그룹코드", "시가총액규모", "지수업종대분류", "지수업종중분류", "지수업종소분류",
    "제조업", "저유동성", "지배구조지수종목", "KOSPI200섹터업종", "KOSPI100",
    "KOSPI50", "KRX", "ETP", "ELW발행", "KRX100",
    "KRX자동차", "KRX반도체", "KRX바이오", "KRX은행", "SPAC",
    "KRX에너지화학", "KRX철강", "단기과열", "KRX미디어통신", "KRX건설",
    "Non1", "KRX증권", "KRX선박", "KRX섹터_보험", "KRX섹터_운송",
    "SRI", "기준가", "매매수량단위", "시간외수량단위", "거래정지",
    "정리매매", "관리종목", "시장경고", "경고예고", "불성실공시",
    "우회상장", "락구분", "액면변경", "증자구분", "증거금비율",
    "신용가능", "신용기간", "전일거래량", "액면가", "상장일자",
    "상장주수", "자본금", "결산월", "공모가", "우선주",
    "공매도과열", "이상급등", "KRX300", "KOSPI", "매출액",
    "영업이익", "경상이익", "당기순이익", "ROE", "기준년월",
    "시가총액", "그룹사코드", "회사신용한도초과", "담보대출가능", "대주가능",
]

KOSDAQ_WIDTHS = [
    2, 1, 4, 4, 4, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 9, 5, 5, 1,
    1, 1, 2, 1, 1, 1, 2, 2, 2, 3,
    1, 3, 12, 12, 8, 15, 21, 2, 7, 1,
    1, 1, 1, 9, 9, 9, 5, 9, 8, 9,
    3, 1, 1, 1,
]
KOSDAQ_COLUMNS = [
    "증권그룹", "시가총액규모", "지수업종대분류", "지수업종중분류", "지수업종소분류",
    "벤처기업", "저유동성종목", "KRX종목여부", "ETP상품", "KRX100여부",
    "KRX자동차", "KRX반도체", "KRX바이오", "KRX은행", "기업인수목적회사",
    "KRX에너지화학", "KRX철강", "단기과열", "KRX미디어통신", "KRX건설",
    "투자주의환기", "KRX증권", "KRX선박", "KRX섹터_보험", "KRX섹터_운송",
    "KOSDAQ150", "기준가", "매매수량단위", "시간외수량단위", "거래정지",
    "정리매매", "관리종목", "시장경고", "경고예고", "불성실공시",
    "우회상장", "락구분", "액면변경", "증자구분", "증거금비율",
    "신용가능", "신용기간", "전일거래량", "액면가", "상장일자",
    "상장주수", "자본금", "결산월", "공모가", "우선주",
    "공매도과열", "이상급등", "KRX300", "매출액", "영업이익",
    "경상이익", "단기순이익", "ROE", "기준년월", "시가총액",
    "그룹사코드", "회사신용한도초과", "담보대출가능", "대주가능",
]


def parse_stock_master(
    mst_path: Path, market: str, part2_width: int
) -> dict[str, dict]:
    """pandas read_fwf로 part2 파싱. 단축코드/한글명/업종/시가총액 추출.

    KOSPI part2 = 228 chars, KOSDAQ part2 = 222 chars. 실제 필드 합은 각각
    227/221이고 첫 1바이트는 패딩이므로 effective 영역은 오른쪽 -1 chars.
    """
    codes: list[str] = []
    names: list[str] = []
    part2_lines: list[str] = []
    effective_width = part2_width - 1  # 선두 패딩 1자 스킵

    with open(mst_path, encoding="cp949") as f:
        for row in f:
            row = row.rstrip("\r\n")
            if len(row) < part2_width + 21:
                continue
            part1 = row[: -part2_width]
            part2 = row[-effective_width:]
            code = part1[0:9].strip()
            name = part1[21:].strip()
            if not code or not name:
                continue
            codes.append(code)
            names.append(name)
            part2_lines.append(part2)

    widths = KOSPI_WIDTHS if part2_width == 228 else KOSDAQ_WIDTHS
    columns = KOSPI_COLUMNS if part2_width == 228 else KOSDAQ_COLUMNS

    df = pd.read_fwf(
        io.StringIO("\n".join(part2_lines)),
        widths=widths,
        names=columns,
        header=None,
        dtype=str,
    )

    result: dict[str, dict] = {}
    for i, code in enumerate(codes):
        row = df.iloc[i]
        sector = str(row.get("지수업종대분류", "") or "").strip()
        cap_raw = str(row.get("시가총액", "") or "").strip()
        try:
            cap_eok = int(cap_raw) if cap_raw and cap_raw.lower() != "nan" else 0
        except ValueError:
            cap_eok = 0
        result[code] = {
            "name": names[i],
            "market": market,
            "sector_code": sector,
            "market_cap_eok": cap_eok,
        }
    return result


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[1/6] sector master → {SECTOR_URL}", file=sys.stderr)
    sector_mst = _download_and_extract(SECTOR_URL, TMP_DIR / "sector")
    sectors = parse_sectors(sector_mst)
    print(f"      {len(sectors)} sectors", file=sys.stderr)

    print(f"[2/6] theme master → {THEME_URL}", file=sys.stderr)
    theme_mst = _download_and_extract(THEME_URL, TMP_DIR / "theme")
    themes, theme_stocks = parse_themes(theme_mst)
    print(
        f"      {len(themes)} themes, {sum(len(v) for v in theme_stocks.values())} rows",
        file=sys.stderr,
    )

    print(f"[3/6] kospi master → {KOSPI_URL}", file=sys.stderr)
    kospi_mst = _download_and_extract(KOSPI_URL, TMP_DIR / "kospi")
    kospi_stocks = parse_stock_master(kospi_mst, market="KOSPI", part2_width=228)
    print(f"      {len(kospi_stocks)} kospi stocks", file=sys.stderr)

    print(f"[4/6] kosdaq master → {KOSDAQ_URL}", file=sys.stderr)
    kosdaq_mst = _download_and_extract(KOSDAQ_URL, TMP_DIR / "kosdaq")
    kosdaq_stocks = parse_stock_master(kosdaq_mst, market="KOSDAQ", part2_width=222)
    print(f"      {len(kosdaq_stocks)} kosdaq stocks", file=sys.stderr)

    print("[5/6] merging stock master …", file=sys.stderr)
    stocks_master = {**kospi_stocks, **kosdaq_stocks}

    print("[6/6] writing JSON files …", file=sys.stderr)
    (DATA_DIR / "sectors.json").write_text(
        json.dumps(sectors, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA_DIR / "themes.json").write_text(
        json.dumps(themes, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA_DIR / "theme_stocks.json").write_text(
        json.dumps(theme_stocks, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA_DIR / "stocks_master.json").write_text(
        json.dumps(stocks_master, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(
        json.dumps(
            {
                "sectors": len(sectors),
                "themes": len(themes),
                "stocks_master": len(stocks_master),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
