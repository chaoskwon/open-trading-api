/**
 * Categories API — 업종/테마/시총 필터 기반 종목 리스트
 */

import { apiGet, apiPost } from "./client";

export interface SectorItem {
  code: string;
  name: string;
  stock_count: number;
}

export interface ThemeItem {
  code: string;
  name: string;
  stock_count: number;
}

export type CapRange = "small" | "mid" | "large";

export interface ResolveRequest {
  sectors: string[];
  themes: string[];
  cap_range: CapRange | null;
}

export interface ResolvedStock {
  code: string;
  name: string;
  market: string;
  sector_code: string;
  market_cap_eok: number;
}

export interface ResolveResponse {
  count: number;
  stocks: ResolvedStock[];
}

export const CAP_RANGE_LABEL: Record<CapRange, string> = {
  small: "1천억~3천억",
  mid: "3천억~1조",
  large: "1조+",
};

export function listSectors(): Promise<SectorItem[]> {
  return apiGet<SectorItem[]>("/api/categories/sectors");
}

export function listThemes(): Promise<ThemeItem[]> {
  return apiGet<ThemeItem[]>("/api/categories/themes");
}

export function resolveStocks(req: ResolveRequest): Promise<ResolveResponse> {
  return apiPost<ResolveResponse>("/api/categories/resolve", req);
}
