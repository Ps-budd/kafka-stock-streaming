"use client";

import { useEffect, useMemo, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { format } from "date-fns";
import FlowInteractive from "@/components/FlowInteractive";

type PricePoint = { ts: string; price: number };
type AlertItem = { symbol: string; price: number; sma: number; zscore: number; rule: string; ts: string };

function computeSMA(points: PricePoint[], windowSize: number): (number | null)[] {
  const out: (number | null)[] = Array(points.length).fill(null);
  let sum = 0;
  for (let i = 0; i < points.length; i++) {
    sum += points[i].price;
    if (i >= windowSize) sum -= points[i - windowSize].price;
    if (i >= windowSize - 1) out[i] = sum / windowSize;
  }
  return out;
}

const DEFAULT_SYMBOLS = ["ALL", "TSLA", "AAPL", "MSFT", "AMZN", "GOOGL"];

export default function Home() {
  const [symbols] = useState<string[]>(DEFAULT_SYMBOLS);
  const [selected, setSelected] = useState<string>(DEFAULT_SYMBOLS[0]);
  const [data, setData] = useState<PricePoint[]>([]);
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [loading, setLoading] = useState(true);

  // Prices: if ALL is selected, chart the first concrete symbol
  useEffect(() => {
    let cancelled = false;
    async function fetchData() {
      try {
        const chartSymbol = selected === "ALL" ? (symbols.find(s => s !== "ALL") || "TSLA") : selected;
        const res = await fetch(`/api/prices?symbol=${encodeURIComponent(chartSymbol)}&limit=500`, { cache: "no-store" });
        const json = await res.json();
        if (!cancelled && json?.data) {
          const points = json.data.map((r: any) => ({ ts: r.ts, price: Number(r.price) }));
          setData(points);
        }
      } catch (e) {
        console.error(e);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    fetchData();
    const id = setInterval(fetchData, 10000);
    return () => { cancelled = true; clearInterval(id); };
  }, [selected, symbols]);

  // Alerts: allow ALL to fetch across all symbols
  useEffect(() => {
    let cancelled = false;
    async function fetchAlerts() {
      try {
        const url = selected === "ALL"
          ? `/api/alerts?limit=50`
          : `/api/alerts?symbol=${encodeURIComponent(selected)}&limit=50`;
        const res = await fetch(url, { cache: "no-store" });
        const json = await res.json();
        if (!cancelled && json?.data) {
          setAlerts(json.data);
        }
      } catch (e) {
        console.error(e);
      }
    }
    fetchAlerts();
    const id = setInterval(fetchAlerts, 10000);
    return () => { cancelled = true; clearInterval(id); };
  }, [selected]);

  const chartData = useMemo(() => {
    const sma = computeSMA(data, 20);
    return data.map((p, i) => ({
      time: format(new Date(p.ts), "HH:mm:ss"),
      price: p.price,
      sma: sma[i],
    }));
  }, [data]);

  return (
    <main className="flex min-h-screen flex-col items-center gap-6 p-8">
      <h1 className="text-2xl font-semibold">Live Prices (with SMA 20)</h1>
      <FlowInteractive />
      <div className="flex items-center gap-3">
        <label className="text-sm text-gray-600">Symbol</label>
        <select
          className="border rounded px-2 py-1"
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
        >
          {symbols.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      </div>
      {loading && <div>Loading…</div>}
      <div className="w-full max-w-5xl h-[400px]">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData} margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
            <XAxis dataKey="time" tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 12 }} domain={["auto", "auto"]} />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="price" stroke="#3b82f6" dot={false} strokeWidth={2} name="Price" />
            <Line type="monotone" dataKey="sma" stroke="#22c55e" dot={false} strokeWidth={2} name="SMA 20" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="w-full max-w-5xl">
        <h2 className="text-lg font-semibold mb-2">Recent Alerts</h2>
        <div className="overflow-x-auto rounded border border-gray-200 dark:border-neutral-800">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 dark:bg-neutral-800">
              <tr>
                <th className="text-left p-2">Time</th>
                <th className="text-left p-2">Symbol</th>
                <th className="text-right p-2">Price</th>
                <th className="text-right p-2">SMA</th>
                <th className="text-right p-2">Z-score</th>
                <th className="text-left p-2">Rule</th>
              </tr>
            </thead>
            <tbody>
              {alerts.map((a, idx) => (
                <tr key={idx} className="border-t border-gray-100 dark:border-neutral-800">
                  <td className="p-2">{format(new Date(a.ts), "HH:mm:ss")}</td>
                  <td className="p-2">{a.symbol}</td>
                  <td className="p-2 text-right">{a.price.toFixed(2)}</td>
                  <td className="p-2 text-right">{a.sma.toFixed(2)}</td>
                  <td className="p-2 text-right">{a.zscore.toFixed(2)}</td>
                  <td className="p-2">{a.rule}</td>
                </tr>
              ))}
              {!alerts.length && (
                <tr><td className="p-3 text-gray-500" colSpan={6}>No alerts yet.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </main>
  );
}
