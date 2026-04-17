"use client";

import { useState, useEffect, useCallback } from "react";
import { Play, Loader2, Target } from "lucide-react";
import {
  StrategySelector,
  SignalList,
  OrderConfirmModal,
  HoldingsList,
  ExecutionLog,
  CategoryFilter,
  OrderResultModal,
} from "@/components/execute";
import { useAuth, useAccount, useStrategyExecutor, useOrder } from "@/hooks";
import {
  getCurrentPrice,
  getBuyableAmount,
  getPendingOrders,
  cancelOrder,
  clearAccountCache,
  type PriceData,
  type PendingOrder,
  type CancelOrderRequest,
} from "@/lib/api";
import type { SignalResult } from "@/types/signal";
import type { OrderRequest, OrderResult } from "@/types/order";
import type { BuyableInfo } from "@/types/account";

export default function MyExecutePage() {
  const { status: authStatus } = useAuth();
  const {
    holdings,
    balance,
    fetchHoldings,
    fetchBalance,
    resetThrottle,
    isLoading: accountLoading,
  } = useAccount();
  const {
    strategies,
    selectedStrategy,
    params,
    signals,
    logs,
    isExecuting,
    error: strategyError,
    selectStrategy,
    setParam,
    execute,
  } = useStrategyExecutor();
  const { execute: executeOrder, isLoading: orderLoading } = useOrder();

  const [stocks, setStocks] = useState<string[]>([]);
  const [selectedSignal, setSelectedSignal] = useState<SignalResult | null>(null);
  const [priceData, setPriceData] = useState<PriceData | null>(null);
  const [buyableInfo, setBuyableInfo] = useState<BuyableInfo | null>(null);
  const [sellableQty, setSellableQty] = useState<number | null>(null);
  const [showOrderModal, setShowOrderModal] = useState(false);

  const [orderResult, setOrderResult] = useState<OrderResult | null>(null);
  const [orderInfo, setOrderInfo] = useState<{
    stock_name: string;
    stock_code: string;
    action: "BUY" | "SELL";
    quantity: number;
    price: number;
  } | null>(null);
  const [showResultModal, setShowResultModal] = useState(false);

  const [pendingOrders, setPendingOrders] = useState<PendingOrder[]>([]);

  const fetchPendingOrders = useCallback(async () => {
    try {
      const response = await getPendingOrders();
      if (response.status === "success") {
        setPendingOrders(response.orders || []);
      }
    } catch (error) {
      console.error("Failed to fetch pending orders:", error);
    }
  }, []);

  useEffect(() => {
    const fetchSequentially = async () => {
      await fetchHoldings();
      await fetchBalance();
      await fetchPendingOrders();
    };
    if (authStatus.authenticated) {
      fetchSequentially();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authStatus.authenticated, authStatus.mode]);

  const handleRefresh = useCallback(async () => {
    resetThrottle();
    await fetchHoldings();
    await fetchBalance();
    await fetchPendingOrders();
  }, [resetThrottle, fetchHoldings, fetchBalance, fetchPendingOrders]);

  const handleCancelOrder = useCallback(
    async (request: CancelOrderRequest) => {
      try {
        const response = await cancelOrder(request);
        if (response.success) {
          await fetchPendingOrders();
          await fetchBalance();
        } else {
          alert(response.message || "주문 취소 실패");
        }
      } catch {
        alert("주문 취소 중 오류가 발생했습니다");
      }
    },
    [fetchPendingOrders, fetchBalance]
  );

  const handleExecute = async () => {
    if (stocks.length === 0) {
      alert("카테고리 필터로 종목을 선택해주세요");
      return;
    }
    await execute(stocks);
  };

  const handleSignalSelect = async (signal: SignalResult) => {
    setSelectedSignal(signal);

    if (signal.action === "BUY" || signal.action === "SELL") {
      try {
        const priceResponse = await getCurrentPrice(signal.code, authStatus.mode);
        if (priceResponse.status === "success" && priceResponse.data) {
          setPriceData(priceResponse.data);
        } else {
          setPriceData(null);
        }

        if (signal.action === "BUY") {
          const buyableResponse = await getBuyableAmount(
            signal.code,
            priceResponse.data?.price || 0
          );
          if (buyableResponse.status === "success" && buyableResponse.data) {
            setBuyableInfo(buyableResponse.data);
          } else {
            setBuyableInfo(null);
          }
          setSellableQty(null);
        } else {
          setBuyableInfo(null);
          const holding = holdings.find((h) => h.stock_code === signal.code);
          setSellableQty(holding?.quantity ?? null);
        }

        setShowOrderModal(true);
      } catch {
        setPriceData(null);
        setBuyableInfo(null);
        setSellableQty(null);
        setShowOrderModal(true);
      }
    }
  };

  const handleOrderConfirm = async (request: OrderRequest) => {
    const result = await executeOrder(request);
    setOrderInfo({
      stock_name: request.stock_name,
      stock_code: request.stock_code,
      action: request.action,
      quantity: request.quantity,
      price: request.price || priceData?.price || 0,
    });
    setOrderResult(result);
    setShowOrderModal(false);
    setSelectedSignal(null);
    setShowResultModal(true);

    await clearAccountCache();
    await new Promise((r) => setTimeout(r, 1500));
    await handleRefresh();
  };

  const handleOrderCancel = () => {
    setShowOrderModal(false);
    setSelectedSignal(null);
    setBuyableInfo(null);
  };

  const handleResultModalClose = () => {
    setShowResultModal(false);
    setOrderResult(null);
    setOrderInfo(null);
  };

  return (
    <>
      <div className="max-w-7xl mx-auto px-4 py-6">
        <div className="mb-6">
          <h1 className="text-display text-slate-900 dark:text-slate-100 flex items-center gap-3">
            <Target className="w-7 h-7 text-primary" />
            나의 전략 실행
          </h1>
          <p className="text-body text-slate-500 dark:text-slate-400 mt-1 ml-10">
            업종/테마/시총 필터로 종목군을 지정하고 전략을 적용합니다
          </p>
        </div>

        {!authStatus.authenticated && (
          <div
            className="card mb-6 border-yellow-200 dark:border-yellow-800 bg-yellow-50 dark:bg-yellow-900/20"
            role="alert"
          >
            <p className="text-body text-yellow-800 dark:text-yellow-200">
              인증이 필요합니다. 우측 상단 설정에서 인증해주세요.
            </p>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left Panel - Strategy & Category Filter */}
          <div className="lg:col-span-1 space-y-6">
            <div className="card p-6">
              <StrategySelector
                strategies={strategies}
                selectedStrategy={selectedStrategy}
                params={params}
                onSelect={selectStrategy}
                onParamChange={setParam}
              />
            </div>

            <div className="card p-6">
              <CategoryFilter onStocksChange={setStocks} />
            </div>

            <button
              onClick={handleExecute}
              disabled={
                !selectedStrategy ||
                stocks.length === 0 ||
                isExecuting ||
                !authStatus.authenticated
              }
              className="w-full flex items-center justify-center gap-2 px-6 py-4 bg-primary text-white rounded-xl hover:bg-primary-dark disabled:opacity-50 disabled:cursor-not-allowed transition-colors font-medium focus-ring"
              aria-label="시그널 생성"
            >
              {isExecuting ? (
                <>
                  <Loader2 className="w-5 h-5 animate-spin" />
                  분석 중... ({stocks.length}종목)
                </>
              ) : (
                <>
                  <Play className="w-5 h-5" />
                  시그널 생성 {stocks.length > 0 && `(${stocks.length}종목)`}
                </>
              )}
            </button>

            {strategyError && (
              <p className="text-caption text-red-500 text-center" role="alert">
                {strategyError}
              </p>
            )}
          </div>

          {/* Center Panel - Signals */}
          <div className="lg:col-span-1 space-y-6">
            <div className="card p-6">
              <h3 className="text-subheading mb-4">시그널 결과</h3>
              <SignalList
                signals={signals}
                onSelect={handleSignalSelect}
                selectedCode={selectedSignal?.code}
              />
            </div>

            {logs.length > 0 && <ExecutionLog logs={logs} maxHeight="300px" />}
          </div>

          {/* Right Panel - Holdings */}
          <div className="lg:col-span-1">
            <HoldingsList
              holdings={holdings}
              pendingOrders={pendingOrders}
              balance={balance}
              onRefresh={handleRefresh}
              onCancelOrder={handleCancelOrder}
              isLoading={accountLoading}
            />
          </div>
        </div>
      </div>

      {showOrderModal && selectedSignal && (
        <OrderConfirmModal
          signal={selectedSignal}
          priceData={priceData}
          buyable={buyableInfo}
          sellableQty={sellableQty}
          onConfirm={handleOrderConfirm}
          onCancel={handleOrderCancel}
          isLoading={orderLoading}
        />
      )}

      {showResultModal && orderResult && orderInfo && (
        <OrderResultModal
          result={orderResult}
          orderInfo={orderInfo}
          onClose={handleResultModalClose}
        />
      )}
    </>
  );
}
