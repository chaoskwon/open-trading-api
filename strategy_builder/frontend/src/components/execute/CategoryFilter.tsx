"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, Loader2, X } from "lucide-react";
import {
  listSectors,
  listThemes,
  resolveStocks,
  CAP_RANGE_LABEL,
  type CapRange,
  type SectorItem,
  type ThemeItem,
  type ResolvedStock,
} from "@/lib/api/categories";

interface CategoryFilterProps {
  onStocksChange: (stocks: string[]) => void;
}

const CAP_OPTIONS: { value: CapRange; label: string }[] = [
  { value: "small", label: CAP_RANGE_LABEL.small },
  { value: "mid", label: CAP_RANGE_LABEL.mid },
  { value: "large", label: CAP_RANGE_LABEL.large },
];

export function CategoryFilter({ onStocksChange }: CategoryFilterProps) {
  const [sectors, setSectors] = useState<SectorItem[]>([]);
  const [themes, setThemes] = useState<ThemeItem[]>([]);
  const [loadingMeta, setLoadingMeta] = useState(true);
  const [metaError, setMetaError] = useState<string | null>(null);

  const [selectedSectors, setSelectedSectors] = useState<string[]>([]);
  const [selectedThemes, setSelectedThemes] = useState<string[]>([]);
  const [capRange, setCapRange] = useState<CapRange | null>(null);

  const [sectorQuery, setSectorQuery] = useState("");
  const [themeQuery, setThemeQuery] = useState("");
  const [sectorOpen, setSectorOpen] = useState(false);
  const [themeOpen, setThemeOpen] = useState(false);

  const [resolved, setResolved] = useState<ResolvedStock[] | null>(null);
  const [resolving, setResolving] = useState(false);
  const [resolveError, setResolveError] = useState<string | null>(null);

  const resolveDebounceRef = useRef<NodeJS.Timeout | null>(null);

  // Load sectors & themes once on mount
  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoadingMeta(true);
      try {
        const [s, t] = await Promise.all([listSectors(), listThemes()]);
        if (cancelled) return;
        setSectors(s);
        setThemes(t);
      } catch (e) {
        if (!cancelled) {
          setMetaError(
            e instanceof Error
              ? e.message
              : "업종/테마 목록 로드 실패. 백엔드 데이터 파일 확인 필요."
          );
        }
      } finally {
        if (!cancelled) setLoadingMeta(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, []);

  // Debounced resolve whenever filters change
  useEffect(() => {
    if (resolveDebounceRef.current) clearTimeout(resolveDebounceRef.current);

    if (
      selectedSectors.length === 0 &&
      selectedThemes.length === 0 &&
      capRange === null
    ) {
      setResolved(null);
      setResolveError(null);
      onStocksChange([]);
      return;
    }

    resolveDebounceRef.current = setTimeout(async () => {
      setResolving(true);
      setResolveError(null);
      try {
        const res = await resolveStocks({
          sectors: selectedSectors,
          themes: selectedThemes,
          cap_range: capRange,
        });
        setResolved(res.stocks);
        onStocksChange(res.stocks.map((s) => s.code));
      } catch (e) {
        setResolveError(e instanceof Error ? e.message : "종목 해석 실패");
        setResolved(null);
        onStocksChange([]);
      } finally {
        setResolving(false);
      }
    }, 250);

    return () => {
      if (resolveDebounceRef.current) clearTimeout(resolveDebounceRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedSectors, selectedThemes, capRange]);

  const toggleSector = useCallback((code: string) => {
    setSelectedSectors((prev) =>
      prev.includes(code) ? prev.filter((c) => c !== code) : [...prev, code]
    );
  }, []);
  const toggleTheme = useCallback((code: string) => {
    setSelectedThemes((prev) =>
      prev.includes(code) ? prev.filter((c) => c !== code) : [...prev, code]
    );
  }, []);

  const removeSector = (code: string) => toggleSector(code);
  const removeTheme = (code: string) => toggleTheme(code);

  const filteredSectors = useMemo(() => {
    const q = sectorQuery.trim().toLowerCase();
    const list = q
      ? sectors.filter(
          (s) =>
            s.name.toLowerCase().includes(q) ||
            s.code.toLowerCase().includes(q)
        )
      : sectors;
    return list.slice(0, 100);
  }, [sectors, sectorQuery]);

  const filteredThemes = useMemo(() => {
    const q = themeQuery.trim().toLowerCase();
    const list = q
      ? themes.filter(
          (t) =>
            t.name.toLowerCase().includes(q) ||
            t.code.toLowerCase().includes(q)
        )
      : themes;
    return list.slice(0, 100);
  }, [themes, themeQuery]);

  const sectorMap = useMemo(
    () => new Map(sectors.map((s) => [s.code, s.name])),
    [sectors]
  );
  const themeMap = useMemo(
    () => new Map(themes.map((t) => [t.code, t.name])),
    [themes]
  );

  if (metaError) {
    return (
      <div className="space-y-3">
        <h3 className="text-subheading">카테고리 필터</h3>
        <p className="text-caption text-red-600" role="alert">
          {metaError}
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <h3 className="text-subheading">카테고리 필터</h3>

      {/* 업종 multi-select */}
      <div>
        <label className="text-caption font-medium text-slate-700 dark:text-slate-300 block mb-2">
          업종 (중복 선택 가능)
        </label>
        <div className="relative">
          <button
            type="button"
            onClick={() => setSectorOpen((v) => !v)}
            disabled={loadingMeta}
            className="w-full flex items-center justify-between px-3 py-2 border border-slate-300 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-body focus-ring disabled:opacity-50"
          >
            <span className="text-slate-600 dark:text-slate-400">
              {loadingMeta
                ? "로딩 중..."
                : selectedSectors.length > 0
                ? `${selectedSectors.length}개 선택됨`
                : "업종 선택"}
            </span>
            <ChevronDown
              className={`w-4 h-4 transition-transform ${sectorOpen ? "rotate-180" : ""}`}
            />
          </button>
          {sectorOpen && !loadingMeta && (
            <div className="absolute z-10 mt-1 w-full max-h-72 overflow-y-auto bg-white dark:bg-slate-800 border border-slate-300 dark:border-slate-700 rounded-lg shadow-lg">
              <input
                type="text"
                value={sectorQuery}
                onChange={(e) => setSectorQuery(e.target.value)}
                placeholder="검색..."
                className="w-full px-3 py-2 border-b border-slate-200 dark:border-slate-700 bg-transparent text-body focus-ring"
              />
              {filteredSectors.length === 0 ? (
                <p className="px-3 py-2 text-caption text-slate-500">결과 없음</p>
              ) : (
                filteredSectors.map((s) => {
                  const checked = selectedSectors.includes(s.code);
                  return (
                    <button
                      key={s.code}
                      type="button"
                      onClick={() => toggleSector(s.code)}
                      className={`w-full text-left px-3 py-2 text-body hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center justify-between ${
                        checked ? "bg-primary/5" : ""
                      }`}
                    >
                      <span>
                        <span className="font-mono text-caption text-slate-400 mr-2">
                          {s.code}
                        </span>
                        {s.name}
                      </span>
                      <span className="text-caption text-slate-500">
                        {s.stock_count}종
                        {checked && " ✓"}
                      </span>
                    </button>
                  );
                })
              )}
            </div>
          )}
        </div>
        {selectedSectors.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-2">
            {selectedSectors.map((code) => (
              <span
                key={code}
                className="inline-flex items-center gap-1 px-2 py-1 bg-primary/10 text-primary rounded text-caption"
              >
                {sectorMap.get(code) ?? code}
                <button
                  onClick={() => removeSector(code)}
                  className="hover:bg-primary/20 rounded p-0.5"
                  aria-label={`${code} 제거`}
                >
                  <X className="w-3 h-3" />
                </button>
              </span>
            ))}
          </div>
        )}
      </div>

      {/* 테마 multi-select */}
      <div>
        <label className="text-caption font-medium text-slate-700 dark:text-slate-300 block mb-2">
          테마 (중복 선택 가능)
        </label>
        <div className="relative">
          <button
            type="button"
            onClick={() => setThemeOpen((v) => !v)}
            disabled={loadingMeta}
            className="w-full flex items-center justify-between px-3 py-2 border border-slate-300 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-body focus-ring disabled:opacity-50"
          >
            <span className="text-slate-600 dark:text-slate-400">
              {loadingMeta
                ? "로딩 중..."
                : selectedThemes.length > 0
                ? `${selectedThemes.length}개 선택됨`
                : "테마 선택"}
            </span>
            <ChevronDown
              className={`w-4 h-4 transition-transform ${themeOpen ? "rotate-180" : ""}`}
            />
          </button>
          {themeOpen && !loadingMeta && (
            <div className="absolute z-10 mt-1 w-full max-h-72 overflow-y-auto bg-white dark:bg-slate-800 border border-slate-300 dark:border-slate-700 rounded-lg shadow-lg">
              <input
                type="text"
                value={themeQuery}
                onChange={(e) => setThemeQuery(e.target.value)}
                placeholder="검색 (예: 반도체, AI, 2차전지)..."
                className="w-full px-3 py-2 border-b border-slate-200 dark:border-slate-700 bg-transparent text-body focus-ring"
              />
              {filteredThemes.length === 0 ? (
                <p className="px-3 py-2 text-caption text-slate-500">결과 없음</p>
              ) : (
                filteredThemes.map((t) => {
                  const checked = selectedThemes.includes(t.code);
                  return (
                    <button
                      key={t.code}
                      type="button"
                      onClick={() => toggleTheme(t.code)}
                      className={`w-full text-left px-3 py-2 text-body hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center justify-between ${
                        checked ? "bg-primary/5" : ""
                      }`}
                    >
                      <span>
                        <span className="font-mono text-caption text-slate-400 mr-2">
                          {t.code}
                        </span>
                        {t.name}
                      </span>
                      <span className="text-caption text-slate-500">
                        {t.stock_count}종
                        {checked && " ✓"}
                      </span>
                    </button>
                  );
                })
              )}
            </div>
          )}
        </div>
        {selectedThemes.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-2">
            {selectedThemes.map((code) => (
              <span
                key={code}
                className="inline-flex items-center gap-1 px-2 py-1 bg-primary/10 text-primary rounded text-caption"
              >
                {themeMap.get(code) ?? code}
                <button
                  onClick={() => removeTheme(code)}
                  className="hover:bg-primary/20 rounded p-0.5"
                  aria-label={`${code} 제거`}
                >
                  <X className="w-3 h-3" />
                </button>
              </span>
            ))}
          </div>
        )}
      </div>

      {/* 시가총액 single-select */}
      <div>
        <label className="text-caption font-medium text-slate-700 dark:text-slate-300 block mb-2">
          시가총액
        </label>
        <div className="grid grid-cols-3 gap-2">
          {CAP_OPTIONS.map((opt) => {
            const selected = capRange === opt.value;
            return (
              <button
                key={opt.value}
                type="button"
                onClick={() => setCapRange(selected ? null : opt.value)}
                className={`px-3 py-2 rounded-lg border text-caption font-medium transition-colors focus-ring ${
                  selected
                    ? "bg-primary text-white border-primary"
                    : "bg-white dark:bg-slate-800 border-slate-300 dark:border-slate-700 text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700"
                }`}
              >
                {opt.label}
              </button>
            );
          })}
        </div>
      </div>

      {/* Resolved preview */}
      <div className="pt-2 border-t border-slate-200 dark:border-slate-700">
        {resolving ? (
          <p className="text-caption text-slate-500 flex items-center gap-1">
            <Loader2 className="w-3 h-3 animate-spin" />
            매칭 중...
          </p>
        ) : resolveError ? (
          <p className="text-caption text-red-600" role="alert">
            {resolveError}
          </p>
        ) : resolved !== null ? (
          <div>
            <p className="text-caption text-slate-600 dark:text-slate-400 mb-1.5">
              매칭: <span className="font-semibold text-primary">{resolved.length}</span>종목
            </p>
            {resolved.length > 0 && (
              <div className="flex flex-wrap gap-1 max-h-28 overflow-y-auto">
                {resolved.slice(0, 40).map((s) => (
                  <span
                    key={s.code}
                    title={`${s.code} · ${s.market_cap_eok.toLocaleString()}억`}
                    className="inline-block px-1.5 py-0.5 bg-slate-100 dark:bg-slate-800 rounded text-caption"
                  >
                    {s.name}
                  </span>
                ))}
                {resolved.length > 40 && (
                  <span className="text-caption text-slate-500">
                    +{resolved.length - 40}개
                  </span>
                )}
              </div>
            )}
          </div>
        ) : (
          <p className="text-caption text-slate-500">
            업종/테마/시총 중 하나 이상 선택하세요.
          </p>
        )}
      </div>
    </div>
  );
}

export default CategoryFilter;
