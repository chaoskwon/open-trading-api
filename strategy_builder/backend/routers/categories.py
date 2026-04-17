"""업종/테마/시총 카테고리 API.

static JSON 파일(backend/data/)을 기반으로 업종 리스트, 테마 리스트,
필터 조건 → 종목코드 리스트 매핑을 제공한다.

데이터 갱신: `uv run backend/scripts/generate_category_data.py` 재실행.
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter(tags=["categories"])

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# 시총 버킷 (억원 단위, [min, max) 반열림)
CAP_BUCKETS: dict[str, tuple[int, int]] = {
    "small": (1000, 3000),       # 1천억~3천억
    "mid": (3000, 10000),        # 3천억~1조
    "large": (10000, 10**12),    # 1조+
}

# 인메모리 캐시 (프로세스 수명 동안 유효)
_cache_lock = threading.Lock()
_cache: dict[str, object] = {}


def _load_json(name: str) -> object:
    path = DATA_DIR / name
    if not path.exists():
        raise HTTPException(
            status_code=503,
            detail=(
                f"데이터 파일 없음: {name}. "
                f"'uv run backend/scripts/generate_category_data.py' 실행 필요."
            ),
        )
    return json.loads(path.read_text(encoding="utf-8"))


def _get_sectors() -> list[dict]:
    with _cache_lock:
        cached = _cache.get("sectors")
        if cached is None:
            cached = _load_json("sectors.json")
            _cache["sectors"] = cached
        return cached  # type: ignore[return-value]


def _get_themes() -> list[dict]:
    with _cache_lock:
        cached = _cache.get("themes")
        if cached is None:
            cached = _load_json("themes.json")
            _cache["themes"] = cached
        return cached  # type: ignore[return-value]


def _get_theme_stocks() -> dict[str, list[str]]:
    with _cache_lock:
        cached = _cache.get("theme_stocks")
        if cached is None:
            cached = _load_json("theme_stocks.json")
            _cache["theme_stocks"] = cached
        return cached  # type: ignore[return-value]


def _get_stocks_master() -> dict[str, dict]:
    with _cache_lock:
        cached = _cache.get("stocks_master")
        if cached is None:
            cached = _load_json("stocks_master.json")
            _cache["stocks_master"] = cached
        return cached  # type: ignore[return-value]


# =============================================================================
# 응답 스키마
# =============================================================================


class SectorItem(BaseModel):
    code: str
    name: str
    stock_count: int = Field(..., description="이 업종에 속한 종목 수 (master 기준)")


class ThemeItem(BaseModel):
    code: str
    name: str
    stock_count: int


class ResolveRequest(BaseModel):
    sectors: list[str] = Field(default_factory=list, description="업종 코드 리스트 (AND 필터)")
    themes: list[str] = Field(default_factory=list, description="테마 코드 리스트 (AND 필터)")
    cap_range: Literal["small", "mid", "large"] | None = Field(
        None, description="시총 버킷. null이면 시총 필터 미적용"
    )


class ResolvedStock(BaseModel):
    code: str
    name: str
    market: str
    sector_code: str
    market_cap_eok: int


class ResolveResponse(BaseModel):
    count: int
    stocks: list[ResolvedStock]


# =============================================================================
# 라우트
# =============================================================================


@router.get("/sectors", response_model=list[SectorItem])
async def list_sectors():
    """업종 리스트 조회. 마스터에 실제로 종목이 속한 업종만 반환."""
    sectors = _get_sectors()
    stocks_master = _get_stocks_master()

    counts: dict[str, int] = {}
    for meta in stocks_master.values():
        sc = (meta or {}).get("sector_code", "")
        if sc:
            counts[sc] = counts.get(sc, 0) + 1

    name_by_code = {s["code"]: s["name"] for s in sectors}
    result: list[SectorItem] = []
    for code, count in counts.items():
        if not code or code == "0000":
            # 미분류 업종 제외
            continue
        name = name_by_code.get(code)
        if not name:
            continue
        result.append(SectorItem(code=code, name=name, stock_count=count))
    result.sort(key=lambda s: (-s.stock_count, s.code))
    return result


@router.get("/themes", response_model=list[ThemeItem])
async def list_themes():
    """테마 리스트 조회. 종목이 1개 이상 속한 테마만."""
    themes = _get_themes()
    theme_stocks = _get_theme_stocks()

    result: list[ThemeItem] = []
    for t in themes:
        code = t["code"]
        count = len(theme_stocks.get(code, []))
        if count > 0:
            result.append(ThemeItem(code=code, name=t["name"], stock_count=count))
    result.sort(key=lambda t: (-t.stock_count, t.code))
    return result


@router.post("/resolve", response_model=ResolveResponse)
async def resolve_stocks(req: ResolveRequest):
    """필터 → 종목 리스트 해석. sectors/themes는 AND, cap_range는 추가 AND."""
    if not req.sectors and not req.themes and req.cap_range is None:
        raise HTTPException(
            status_code=400,
            detail="sectors, themes, cap_range 중 최소 하나는 선택해야 합니다.",
        )

    stocks_master = _get_stocks_master()
    theme_stocks = _get_theme_stocks()

    # 1) 초기 후보 집합 선정
    candidate: set[str] | None = None

    if req.sectors:
        sec_set = set(req.sectors)
        sector_matched = {
            code
            for code, meta in stocks_master.items()
            if meta.get("sector_code") in sec_set
        }
        candidate = sector_matched

    if req.themes:
        theme_matched: set[str] = set()
        for tcode in req.themes:
            for sc in theme_stocks.get(tcode, []):
                theme_matched.add(sc)
        candidate = theme_matched if candidate is None else candidate & theme_matched

    if candidate is None:
        # sectors/themes 둘 다 비어있고 cap_range만 있으면 전체 마스터 대상
        candidate = set(stocks_master.keys())

    # 2) 시총 필터
    if req.cap_range is not None:
        lo, hi = CAP_BUCKETS[req.cap_range]
        candidate = {
            c
            for c in candidate
            if (meta := stocks_master.get(c))
            and lo <= (meta.get("market_cap_eok") or 0) < hi
        }

    # 3) 마스터에 없는 종목 (ETF 등) 은 제외
    resolved: list[ResolvedStock] = []
    for code in candidate:
        meta = stocks_master.get(code)
        if not meta:
            continue
        resolved.append(
            ResolvedStock(
                code=code,
                name=meta.get("name", ""),
                market=meta.get("market", ""),
                sector_code=meta.get("sector_code", ""),
                market_cap_eok=meta.get("market_cap_eok", 0),
            )
        )

    resolved.sort(key=lambda s: s.market_cap_eok, reverse=True)

    return ResolveResponse(count=len(resolved), stocks=resolved)
