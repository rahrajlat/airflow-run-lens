import React, { useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  ArrowDown,
  ArrowUp,
  CheckCircle2,
  Clock3,
  GitCompareArrows,
  Loader2,
  RefreshCw,
  RotateCcw,
  XCircle
} from "lucide-react";

type DagRun = {
  dag_id: string;
  run_id: string;
  state: string | null;
  logical_date: string | null;
  start_date: string | null;
  end_date: string | null;
  duration_seconds: number | null;
};

type TaskSnapshot = {
  task_id: string;
  state: string | null;
  try_number: number | null;
  duration_seconds: number | null;
};

type TaskComparison = {
  task_id: string;
  base: TaskSnapshot | null;
  compare: TaskSnapshot | null;
  duration_delta_seconds: number | null;
  duration_delta_percent: number | null;
  retry_delta: number | null;
};

type ComparePayload = {
  dag_id: string;
  base_run: DagRun;
  compare_run: DagRun;
  summary: {
    total_duration: {
      base_seconds: number | null;
      compare_seconds: number | null;
      delta_percent: number | null;
    };
    succeeded_tasks: { base: number; compare: number };
    failed_tasks: { base: number; compare: number };
    total_retries: { base: number; compare: number };
  };
  tasks: TaskComparison[];
};

type HistoryTask = {
  task_id: string;
  latest: TaskSnapshot | null;
  median_seconds: number | null;
  latest_delta_seconds: number | null;
  latest_delta_percent: number | null;
  history: TaskSnapshot[];
};

type HistoryPayload = {
  dag_id: string;
  days: number;
  runs: DagRun[];
  tasks: HistoryTask[];
  summary: {
    run_count?: number;
    success_count?: number;
    failed_count?: number;
    latest_run_id?: string;
  };
};

type RunLensProps = {
  dagId?: string;
  dag_id?: string;
  DAG_ID?: string;
  context?: {
    dagId?: string;
    dag_id?: string;
    DAG_ID?: string;
  };
};

declare global {
  interface Window {
    Airflow?: {
      dagId?: string;
      dag_id?: string;
      context?: RunLensProps["context"];
    };
  }
}

const style = document.createElement("style");
style.textContent = `
  .runlens-root { color: var(--chakra-colors-fg, #1a202c); font-family: inherit; padding: 16px 24px 28px; }
  .runlens-shell { display: grid; gap: 14px; max-width: 100%; }
  .runlens-title-row { align-items: center; display: grid; gap: 12px; grid-template-columns: 1fr auto; }
  .runlens-kicker { color: var(--chakra-colors-blue-500, #017cee); font-size: 11px; font-weight: 700; margin: 0 0 2px; text-transform: uppercase; }
  .runlens-title { font-size: 22px; font-weight: 700; letter-spacing: 0; line-height: 1.2; margin: 0; }
  .runlens-subtitle { color: var(--chakra-colors-gray-600, #5f6b7a); font-size: 13px; margin: 4px 0 0; }
  .runlens-button { align-items: center; background: var(--chakra-colors-blue-500, #017cee); border: 1px solid var(--chakra-colors-blue-600, #0063bf); border-radius: 6px; color: #fff; cursor: pointer; display: inline-flex; font-size: 13px; font-weight: 600; gap: 7px; min-height: 32px; padding: 0 12px; }
  .runlens-button:disabled { cursor: not-allowed; opacity: 0.62; }
  .runlens-panel { background: var(--chakra-colors-bg-panel, #fff); border: 1px solid var(--chakra-colors-border, #d8dee4); border-radius: 6px; box-shadow: none; }
  .runlens-toolbar { display: grid; gap: 14px; grid-template-columns: minmax(180px, 1fr) minmax(220px, 1fr) minmax(220px, 1fr); padding: 12px; }
  .runlens-filterbar { align-items: end; display: grid; gap: 14px; grid-template-columns: minmax(240px, 0.45fr) minmax(220px, 0.28fr) 1fr; padding: 12px; }
  .runlens-history-grid { display: grid; gap: 12px; grid-template-columns: 1fr; }
  .runlens-history-header { align-items: center; display: flex; gap: 12px; justify-content: space-between; padding: 12px 14px; }
  .runlens-field { display: grid; gap: 6px; }
  .runlens-label { color: var(--chakra-colors-gray-700, #2d3748); font-size: 12px; font-weight: 600; }
  .runlens-select { appearance: none; background: var(--chakra-colors-bg, #fff); border: 1px solid var(--chakra-colors-border, #cfd7df); border-radius: 6px; color: inherit; font: inherit; height: 34px; padding: 0 10px; }
  .runlens-card-grid { display: grid; gap: 12px; grid-template-columns: repeat(4, minmax(0, 1fr)); }
  .runlens-metric { padding: 13px 14px; }
  .runlens-metric-top { align-items: center; display: flex; gap: 8px; font-size: 13px; font-weight: 650; margin-bottom: 12px; }
  .runlens-metric-values { align-items: end; display: grid; gap: 12px; grid-template-columns: 1fr 1fr auto; }
  .runlens-value { font-size: 22px; font-weight: 700; line-height: 1; }
  .runlens-caption { color: var(--chakra-colors-gray-500, #718096); font-size: 11px; margin-top: 6px; }
  .runlens-pill { align-items: center; border-radius: 999px; display: inline-flex; font-size: 12px; font-weight: 650; gap: 4px; min-height: 24px; padding: 0 9px; }
  .runlens-pill-up { background: #fde8e8; color: #c53030; }
  .runlens-pill-down { background: #e6f6ef; color: #047857; }
  .runlens-pill-flat { background: var(--chakra-colors-gray-100, #edf2f7); color: var(--chakra-colors-gray-700, #2d3748); }
  .runlens-content-grid { display: grid; gap: 12px; grid-template-columns: minmax(0, 1.55fr) minmax(340px, 0.45fr); }
  .runlens-section-title { align-items: center; border-bottom: 1px solid var(--chakra-colors-border, #e2e8f0); display: flex; font-size: 15px; font-weight: 700; gap: 8px; margin: 0; padding: 12px 14px; }
  .runlens-table { overflow: auto; }
  .runlens-table-head, .runlens-table-row { display: grid; grid-template-columns: minmax(190px, 1.3fr) repeat(6, minmax(110px, 1fr)); min-width: 900px; }
  .runlens-table-head > div { background: var(--chakra-colors-gray-50, #f7fafc); color: var(--chakra-colors-gray-700, #2d3748); font-size: 12px; font-weight: 650; padding: 10px 12px; }
  .runlens-table-row > div { border-top: 1px solid var(--chakra-colors-border, #e2e8f0); font-size: 13px; padding: 9px 12px; }
  .runlens-status { border-radius: 999px; display: inline-flex; font-size: 12px; font-weight: 650; padding: 3px 8px; }
  .runlens-status-success { background: #dff5e8; color: #047857; }
  .runlens-status-failed { background: #fde8e8; color: #c53030; }
  .runlens-status-missing { background: var(--chakra-colors-gray-100, #edf2f7); color: var(--chakra-colors-gray-500, #718096); }
  .runlens-status-other { background: #e6f2ff; color: #0b66c3; }
  .runlens-timeline { display: grid; gap: 12px; padding: 14px; }
  .runlens-timeline-label { color: var(--chakra-colors-gray-700, #2d3748); font-size: 12px; font-weight: 650; margin-bottom: 7px; }
  .runlens-bar-track { background: var(--chakra-colors-gray-100, #edf2f7); border: 1px solid var(--chakra-colors-border, #e2e8f0); border-radius: 6px; height: 24px; overflow: hidden; }
  .runlens-bar { align-items: center; background: var(--chakra-colors-teal-500, #00a3a3); color: #fff; display: flex; font-size: 11px; font-weight: 650; height: 100%; justify-content: center; min-width: 3px; white-space: nowrap; }
  .runlens-runbar { display: grid; gap: 8px; grid-template-columns: minmax(190px, 0.36fr) minmax(160px, 1fr) 74px; padding: 9px 14px; }
  .runlens-runbar + .runlens-runbar { border-top: 1px solid var(--chakra-colors-border, #e2e8f0); }
  .runlens-runbar-label { color: var(--chakra-colors-gray-700, #2d3748); font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .runlens-history-chart { display: grid; gap: 14px; padding: 14px; }
  .runlens-history-chart-row { display: grid; gap: 8px; }
  .runlens-history-chart-label { align-items: center; display: flex; justify-content: space-between; color: var(--chakra-colors-gray-700, #2d3748); font-size: 12px; font-weight: 650; }
  .runlens-history-bar-latest { background: var(--chakra-colors-teal-500, #00a3a3); }
  .runlens-history-bar-median { background: var(--chakra-colors-blue-500, #017cee); }
  .runlens-empty, .runlens-error { border: 1px dashed var(--chakra-colors-border, #cbd5e1); border-radius: 6px; color: var(--chakra-colors-gray-600, #4a5568); font-size: 14px; padding: 16px; }
  .runlens-error { background: #fff1f2; border-color: #fecdd3; color: #9f1239; }
  @media (max-width: 1050px) { .runlens-toolbar, .runlens-filterbar, .runlens-card-grid, .runlens-content-grid, .runlens-history-grid { grid-template-columns: 1fr; } }
`;
if (!document.getElementById("runlens-styles")) {
  style.id = "runlens-styles";
  document.head.appendChild(style);
}

function getDagId(props: RunLensProps): string | null {
  const fromProps = props.dagId || props.dag_id || props.DAG_ID || props.context?.dagId || props.context?.dag_id || props.context?.DAG_ID;
  if (fromProps) return fromProps;
  const airflow = window.Airflow;
  const fromAirflow = airflow?.dagId || airflow?.dag_id || airflow?.context?.dagId || airflow?.context?.dag_id || airflow?.context?.DAG_ID;
  if (fromAirflow) return fromAirflow;
  const queryDagId = new URLSearchParams(window.location.search).get("dag_id");
  if (queryDagId) return queryDagId;
  const match = window.location.pathname.match(/\/dags\/([^/]+)/);
  return match ? decodeURIComponent(match[1]) : null;
}

function formatSeconds(value: number | null | undefined): string {
  if (value === null || value === undefined) return "-";
  const seconds = Math.round(value);
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return minutes > 0 ? `${minutes}m ${remainder}s` : `${remainder}s`;
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "0%";
  return `${Math.round(value)}%`;
}

function shortRunLabel(run: DagRun): string {
  return run.run_id.startsWith("manual__") ? `manual ${run.run_id.slice(8, 27).replace("T", " ")}` : run.run_id;
}

function DeltaPill({ value }: { value: number | null | undefined }) {
  const Icon = value && value > 0 ? ArrowUp : value && value < 0 ? ArrowDown : GitCompareArrows;
  const className = !value ? "runlens-pill runlens-pill-flat" : value > 0 ? "runlens-pill runlens-pill-up" : "runlens-pill runlens-pill-down";
  return <span className={className}><Icon size={13} />{formatPercent(value)}</span>;
}

function StatePill({ state }: { state: string | null | undefined }) {
  if (state === undefined) {
    return <span className="runlens-status runlens-status-missing">missing</span>;
  }
  const normalized = state || "unknown";
  const className = normalized === "success" ? "runlens-status runlens-status-success" : normalized === "failed" ? "runlens-status runlens-status-failed" : "runlens-status runlens-status-other";
  return <span className={className}>{normalized}</span>;
}

function retryCount(task: TaskSnapshot | null): number | null {
  if (!task) return null;
  return Math.max((task.try_number || 1) - 1, 0);
}

function RetryTransition({ task }: { task: TaskComparison }) {
  const baseRetries = retryCount(task.base);
  const compareRetries = retryCount(task.compare);
  return <span>{baseRetries ?? "-"} → {compareRetries ?? "-"}</span>;
}

async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(`/runlens/api${path}`, { credentials: "same-origin", headers: { Accept: "application/json" } });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json() as Promise<T>;
}

function MetricCard({ icon, title, base, compare, delta }: { icon: React.ReactNode; title: string; base: string; compare: string; delta?: number | null }) {
  return (
    <section className="runlens-panel runlens-metric">
      <div className="runlens-metric-top">{icon}{title}</div>
      <div className="runlens-metric-values">
        <div><div className="runlens-value">{base}</div><div className="runlens-caption">Base run</div></div>
        <div><div className="runlens-value">{compare}</div><div className="runlens-caption">Compare run</div></div>
        {delta !== undefined ? <DeltaPill value={delta} /> : null}
      </div>
    </section>
  );
}

export default function RunLensApp(props: RunLensProps = {}) {
  const dagId = useMemo(() => getDagId(props), [props]);
  const [runs, setRuns] = useState<DagRun[]>([]);
  const [baseRunId, setBaseRunId] = useState("");
  const [compareRunId, setCompareRunId] = useState("");
  const [taskFilter, setTaskFilter] = useState("all");
  const [presenceFilter, setPresenceFilter] = useState("all");
  const [historyDays, setHistoryDays] = useState(10);
  const [historyTaskId, setHistoryTaskId] = useState("");
  const [history, setHistory] = useState<HistoryPayload | null>(null);
  const [comparison, setComparison] = useState<ComparePayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadRuns() {
    if (!dagId) {
      setError("Unable to detect the current DAG id from Airflow.");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const payload = await apiGet<{ runs: DagRun[] }>(`/dags/${encodeURIComponent(dagId)}/runs?limit=30`);
      setRuns(payload.runs);
      const first = payload.runs[0]?.run_id || "";
      const second = payload.runs[1]?.run_id || "";
      setCompareRunId((current) => current || first);
      setBaseRunId((current) => current || second || first);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load DAG runs.");
    } finally {
      setLoading(false);
    }
  }

  async function loadComparison(nextBase = baseRunId, nextCompare = compareRunId) {
    if (!dagId || !nextBase || !nextCompare) return;
    setLoading(true);
    setError(null);
    try {
      const payload = await apiGet<ComparePayload>(`/dags/${encodeURIComponent(dagId)}/compare?base_run_id=${encodeURIComponent(nextBase)}&compare_run_id=${encodeURIComponent(nextCompare)}`);
      setComparison(payload);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to compare DAG runs.");
    } finally {
      setLoading(false);
    }
  }

  async function loadHistory(days = historyDays) {
    if (!dagId) return;
    try {
      const payload = await apiGet<HistoryPayload>(`/dags/${encodeURIComponent(dagId)}/history?days=${days}`);
      setHistory(payload);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load DAG history.");
    }
  }

  useEffect(() => { loadRuns(); }, [dagId]);
  useEffect(() => { if (baseRunId && compareRunId) loadComparison(baseRunId, compareRunId); }, [baseRunId, compareRunId]);
  useEffect(() => { loadHistory(historyDays); }, [dagId, historyDays]);
  useEffect(() => {
    setTaskFilter("all");
    setPresenceFilter("all");
  }, [comparison?.base_run.run_id, comparison?.compare_run.run_id]);

  const taskOptions = comparison?.tasks.map((task) => task.task_id).sort() || [];
  const filteredTasks = (comparison?.tasks || []).filter((task) => {
    const taskMatches = taskFilter === "all" || task.task_id === taskFilter;
    const presenceMatches =
      presenceFilter === "all" ||
      (presenceFilter === "both" && task.base && task.compare) ||
      (presenceFilter === "base_only" && task.base && !task.compare) ||
      (presenceFilter === "compare_only" && !task.base && task.compare) ||
      (presenceFilter === "changed" && (!task.base || !task.compare));
    return taskMatches && presenceMatches;
  });
  const maxTaskDuration = Math.max(1, ...(filteredTasks.flatMap((task) => [task.base?.duration_seconds || 0, task.compare?.duration_seconds || 0]) || [1]));
  const historyRunMaxDuration = Math.max(1, ...(history?.runs.map((run) => run.duration_seconds || 0) || [1]));
  const historicalTaskDeltas = (history?.tasks || [])
    .filter((task) => task.latest_delta_seconds !== null)
    .sort((a, b) => (b.latest_delta_seconds || 0) - (a.latest_delta_seconds || 0))
    .slice(0, 8);
  const historyTaskOptions = (history?.tasks || []).map((task) => task.task_id).sort();
  const selectedHistoryTask =
    (history?.tasks || []).find((task) => task.task_id === historyTaskId) ||
    historicalTaskDeltas[0] ||
    history?.tasks[0] ||
    null;
  const historyCompareMax = Math.max(
    1,
    selectedHistoryTask?.latest?.duration_seconds || 0,
    selectedHistoryTask?.median_seconds || 0
  );

  return (
    <main className="runlens-root">
      <div className="runlens-shell">
        <header className="runlens-title-row">
          <div>
            <p className="runlens-kicker">{dagId || "DAG context missing"}</p>
            <h1 className="runlens-title">RunLens</h1>
            <p className="runlens-subtitle">Compare DAG runs, spot runtime regressions, and inspect task-level timing.</p>
          </div>
          <button className="runlens-button" onClick={() => loadRuns()} disabled={loading}>{loading ? <Loader2 size={17} /> : <RefreshCw size={17} />}Refresh</button>
        </header>
        <section className="runlens-panel runlens-toolbar">
          <label className="runlens-field"><span className="runlens-label">DAG</span><select className="runlens-select" value={dagId || ""} disabled><option>{dagId || "Unknown DAG"}</option></select></label>
          <label className="runlens-field"><span className="runlens-label">Base Run</span><select className="runlens-select" value={baseRunId} onChange={(event) => setBaseRunId(event.target.value)}>{runs.map((run) => <option key={run.run_id} value={run.run_id}>{shortRunLabel(run)}</option>)}</select></label>
          <label className="runlens-field"><span className="runlens-label">Compare Run</span><select className="runlens-select" value={compareRunId} onChange={(event) => setCompareRunId(event.target.value)}>{runs.map((run) => <option key={run.run_id} value={run.run_id}>{shortRunLabel(run)}</option>)}</select></label>
        </section>
        {error ? <div className="runlens-error">{error}</div> : null}
        {comparison ? (
          <>
            <section className="runlens-card-grid">
              <MetricCard icon={<Clock3 color="#1d4ed8" size={22} />} title="Total Duration" base={formatSeconds(comparison.summary.total_duration.base_seconds)} compare={formatSeconds(comparison.summary.total_duration.compare_seconds)} delta={comparison.summary.total_duration.delta_percent} />
              <MetricCard icon={<CheckCircle2 color="#16a34a" size={22} />} title="Succeeded Tasks" base={String(comparison.summary.succeeded_tasks.base)} compare={String(comparison.summary.succeeded_tasks.compare)} />
              <MetricCard icon={<XCircle color="#dc2626" size={22} />} title="Failed Tasks" base={String(comparison.summary.failed_tasks.base)} compare={String(comparison.summary.failed_tasks.compare)} />
              <MetricCard icon={<RotateCcw color="#ea580c" size={22} />} title="Total Retries" base={String(comparison.summary.total_retries.base)} compare={String(comparison.summary.total_retries.compare)} />
            </section>
            <section className="runlens-panel runlens-filterbar">
              <label className="runlens-field">
                <span className="runlens-label">Task</span>
                <select className="runlens-select" value={taskFilter} onChange={(event) => setTaskFilter(event.target.value)}>
                  <option value="all">All tasks</option>
                  {taskOptions.map((taskId) => <option key={taskId} value={taskId}>{taskId}</option>)}
                </select>
              </label>
              <label className="runlens-field">
                <span className="runlens-label">Presence</span>
                <select className="runlens-select" value={presenceFilter} onChange={(event) => setPresenceFilter(event.target.value)}>
                  <option value="all">All</option>
                  <option value="both">In both runs</option>
                  <option value="changed">Missing in either run</option>
                  <option value="base_only">Base only</option>
                  <option value="compare_only">Compare only</option>
                </select>
              </label>
              <div className="runlens-caption">{filteredTasks.length} of {comparison.tasks.length} tasks shown</div>
            </section>
            <section className="runlens-content-grid">
              <div className="runlens-panel">
                <h2 className="runlens-section-title"><GitCompareArrows size={18} />Task Comparison</h2>
                <div className="runlens-table">
                  <div className="runlens-table-head"><div>Task ID</div><div>Base Duration</div><div>Compare Duration</div><div>Delta</div><div>Base State</div><div>Compare State</div><div>Retries</div></div>
                  {filteredTasks.map((task) => <div className="runlens-table-row" key={task.task_id}><div>{task.task_id}</div><div>{task.base ? formatSeconds(task.base.duration_seconds) : "-"}</div><div>{task.compare ? formatSeconds(task.compare.duration_seconds) : "-"}</div><div>{task.base && task.compare ? <DeltaPill value={task.duration_delta_percent} /> : <span className="runlens-status runlens-status-missing">n/a</span>}</div><div><StatePill state={task.base?.state} /></div><div><StatePill state={task.compare?.state} /></div><div><RetryTransition task={task} /></div></div>)}
                  {filteredTasks.length === 0 ? <div className="runlens-empty">No tasks match the current filters.</div> : null}
                </div>
              </div>
              <div className="runlens-panel">
                <h2 className="runlens-section-title"><AlertCircle size={18} />Duration Hotspots</h2>
                <div className="runlens-timeline">
                  {filteredTasks.slice().sort((a, b) => (b.duration_delta_seconds || 0) - (a.duration_delta_seconds || 0)).slice(0, 8).map((task) => {
                    const duration = task.compare?.duration_seconds || 0;
                    return <div key={task.task_id}><div className="runlens-timeline-label">{task.task_id}</div><div className="runlens-bar-track"><div className="runlens-bar" style={{ width: `${Math.max(4, (duration / maxTaskDuration) * 100)}%` }}>{formatSeconds(duration)}</div></div></div>;
                  })}
                </div>
              </div>
            </section>
          </>
        ) : <div className="runlens-empty">{loading ? "Loading RunLens data..." : "Trigger this DAG at least twice to compare runs."}</div>}
        <section className="runlens-panel">
          <div className="runlens-history-header">
            <h2 className="runlens-section-title" style={{ borderBottom: "0", padding: 0 }}><Clock3 size={18} />Historical View</h2>
            <label className="runlens-field" style={{ minWidth: "180px" }}>
              <span className="runlens-label">Window</span>
              <select className="runlens-select" value={historyDays} onChange={(event) => setHistoryDays(Number(event.target.value))}>
                <option value={10}>Last 10 days</option>
                <option value={30}>Last 30 days</option>
                <option value={60}>Last 60 days</option>
                <option value={90}>Last 90 days</option>
              </select>
            </label>
          </div>
          {history && history.runs.length > 0 ? (
            <div className="runlens-history-grid">
              <div className="runlens-panel">
                <h3 className="runlens-section-title"><Clock3 size={16} />Run Duration Trend</h3>
                {history.runs.map((run) => {
                  const duration = run.duration_seconds || 0;
                  const width = `${Math.max(3, (duration / historyRunMaxDuration) * 100)}%`;
                  return (
                    <div className="runlens-runbar" key={run.run_id}>
                      <div className="runlens-runbar-label" title={run.run_id}>{shortRunLabel(run)}</div>
                      <div className="runlens-bar-track"><div className="runlens-bar" style={{ width }}>{formatSeconds(duration)}</div></div>
                      <StatePill state={run.state || undefined} />
                    </div>
                  );
                })}
              </div>
              <div className="runlens-panel">
                <div className="runlens-history-header">
                  <h3 className="runlens-section-title" style={{ borderBottom: "0", padding: 0 }}><AlertCircle size={16} />Latest vs Median</h3>
                  <label className="runlens-field" style={{ minWidth: "190px" }}>
                    <span className="runlens-label">Task</span>
                    <select className="runlens-select" value={selectedHistoryTask?.task_id || ""} onChange={(event) => setHistoryTaskId(event.target.value)}>
                      {historyTaskOptions.map((taskId) => <option key={taskId} value={taskId}>{taskId}</option>)}
                    </select>
                  </label>
                </div>
                {selectedHistoryTask ? (
                  <div className="runlens-history-chart">
                    <div className="runlens-timeline-label">{selectedHistoryTask.task_id}</div>
                    <div className="runlens-history-chart-row">
                      <div className="runlens-history-chart-label"><span>Latest</span><span>{formatSeconds(selectedHistoryTask.latest?.duration_seconds)}</span></div>
                      <div className="runlens-bar-track"><div className="runlens-bar runlens-history-bar-latest" style={{ width: `${Math.max(4, ((selectedHistoryTask.latest?.duration_seconds || 0) / historyCompareMax) * 100)}%` }} /></div>
                    </div>
                    <div className="runlens-history-chart-row">
                      <div className="runlens-history-chart-label"><span>Median</span><span>{formatSeconds(selectedHistoryTask.median_seconds)}</span></div>
                      <div className="runlens-bar-track"><div className="runlens-bar runlens-history-bar-median" style={{ width: `${Math.max(4, ((selectedHistoryTask.median_seconds || 0) / historyCompareMax) * 100)}%` }} /></div>
                    </div>
                    <div className="runlens-caption">Delta <DeltaPill value={selectedHistoryTask.latest_delta_percent} /></div>
                  </div>
                ) : <div className="runlens-empty">No historical task data in this window.</div>}
              </div>
            </div>
          ) : <div className="runlens-empty">No DAG runs found in the last {historyDays} days.</div>}
        </section>
      </div>
    </main>
  );
}

(globalThis as any)["RunLens"] = RunLensApp;
(globalThis as any).AirflowPlugin = RunLensApp;
