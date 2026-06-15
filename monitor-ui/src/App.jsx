import { useState, useEffect, useCallback } from 'react'

const API = 'http://localhost:8765'
const CHAINS = ['All', 'New World', "PAK'nSAVE", 'Woolworths']

function chainStyle(chain = '') {
  if (chain.toLowerCase().includes('new world')) return { background: '#064e3b', color: '#34d399' }
  if (chain.toLowerCase().includes('pak')) return { background: '#451a03', color: '#fb923c' }
  return { background: '#1e1b4b', color: '#818cf8' }
}

function statusColor(s) {
  return s === 'success' ? '#22c55e' : s === 'partial' ? '#f59e0b' : '#ef4444'
}

function catStyle(s) {
  if (s === 'success') return { background: 'rgba(20,83,45,.3)', border: 'rgba(22,101,52,.4)', color: '#4ade80' }
  if (s === 'failed')  return { background: 'rgba(69,10,10,.3)', border: 'rgba(127,29,29,.4)', color: '#f87171' }
  return { background: 'rgba(30,41,59,.5)', border: '#2d3148', color: '#64748b' }
}

function fmt(n) { return n >= 1000 ? (n / 1000).toFixed(1) + 'k' : String(n) }

function timeAgo(iso) {
  const s = Math.round((Date.now() - new Date(iso)) / 1000)
  if (s < 60) return `${s}s ago`
  if (s < 3600) return `${Math.floor(s / 60)}m ago`
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m ago`
}

function BranchCard({ report }) {
  const [open, setOpen] = useState(false)
  const sc = statusColor(report.status)
  const cs = chainStyle(report.chain)
  const failedCats = report.categories.filter(c => c.status === 'failed')

  return (
    <div style={{ background: '#1a1d2e', border: '1px solid #2d3148', borderLeft: `3px solid ${sc}`, borderRadius: 10, overflow: 'hidden' }}>
      {/* Row */}
      <div onClick={() => setOpen(o => !o)} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '13px 18px', cursor: 'pointer' }}>
        <span style={{ fontSize: 11, padding: '2px 8px', borderRadius: 4, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '.04em', ...cs }}>{report.chain}</span>
        <span style={{ fontWeight: 600, fontSize: 14 }}>{report.branch_name}</span>
        {failedCats.length > 0 && <span style={{ fontSize: 11, color: '#ef4444' }}>{failedCats.length} fail{failedCats.length > 1 ? 's' : ''}</span>}
        <span style={{ marginLeft: 'auto', fontSize: 11, fontWeight: 600, padding: '3px 10px', borderRadius: 12, textTransform: 'uppercase', background: `${sc}22`, color: sc }}>{report.status}</span>
        <span style={{ color: '#475569', fontSize: 11, marginLeft: 6, display: 'inline-block', transform: open ? 'rotate(90deg)' : 'none', transition: 'transform .2s' }}>▶</span>
      </div>

      {/* Detail */}
      {open && (
        <div style={{ padding: '0 18px 16px', borderTop: '1px solid #2d3148' }}>
          <div style={{ display: 'flex', gap: 28, padding: '12px 0 14px', flexWrap: 'wrap' }}>
            {[
              { label: 'Products', value: fmt(report.total_products), color: '#fff' },
              { label: 'Price Changes', value: fmt(report.price_changes), color: '#fff' },
              { label: 'Specials', value: fmt(report.specials), color: '#fbbf24' },
              { label: 'Out of Stock', value: fmt(report.out_of_stock), color: '#f87171' },
              { label: 'Categories', value: report.categories.length, color: '#fff' },
            ].map(m => (
              <div key={m.label} style={{ textAlign: 'center' }}>
                <div style={{ fontSize: 20, fontWeight: 700, color: m.color }}>{m.value}</div>
                <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>{m.label}</div>
              </div>
            ))}
          </div>

          <div style={{ fontSize: 11, color: '#64748b', textTransform: 'uppercase', letterSpacing: '.05em', marginBottom: 8 }}>Categories</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {report.categories.map((cat, i) => {
              const s = catStyle(cat.status)
              return (
                <div key={i} title={cat.reason || cat.status} style={{ display: 'inline-flex', alignItems: 'center', gap: 5, fontSize: 12, padding: '3px 10px', borderRadius: 4, background: s.background, border: `1px solid ${s.border}`, color: s.color }}>
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: s.color, flexShrink: 0 }} />
                  {cat.name}
                  {cat.products > 0 && <span style={{ opacity: .6 }}>({cat.products})</span>}
                  {cat.reason && <span style={{ fontSize: 10, color: '#94a3b8', marginLeft: 2 }}>· {cat.reason.substring(0, 35)}</span>}
                </div>
              )
            })}
          </div>

          <div style={{ fontSize: 11, color: '#475569', marginTop: 12 }}>
            {new Date(report.timestamp).toLocaleString()} · {timeAgo(report.timestamp)}
            {report.branch_id && ` · ${report.branch_id}`}
          </div>
        </div>
      )}
    </div>
  )
}

export default function App() {
  const [reports, setReports] = useState([])
  const [chain, setChain] = useState('All')
  const [apiStatus, setApiStatus] = useState('connecting')
  const [lastUpdate, setLastUpdate] = useState(null)

  const poll = useCallback(async () => {
    try {
      const res = await fetch(`${API}/reports`)
      if (!res.ok) throw new Error()
      setReports(await res.json())
      setApiStatus('ok')
      setLastUpdate(new Date())
    } catch {
      setApiStatus('error')
    }
  }, [])

  useEffect(() => {
    poll()
    const id = setInterval(poll, 5000)
    return () => clearInterval(id)
  }, [poll])

  const filtered = chain === 'All' ? reports : reports.filter(r => r.chain === chain)
  const sorted = [...filtered].reverse()

  const total = reports.reduce((a, r) => a + r.total_products, 0)
  const prices = reports.reduce((a, r) => a + r.price_changes, 0)
  const specials = reports.reduce((a, r) => a + r.specials, 0)
  const oos = reports.reduce((a, r) => a + r.out_of_stock, 0)
  const catFails = reports.reduce((a, r) => a + r.categories.filter(c => c.status === 'failed').length, 0)

  const dotColor = apiStatus === 'ok' ? '#22c55e' : apiStatus === 'error' ? '#ef4444' : '#f59e0b'

  return (
    <div style={{ minHeight: '100vh', background: '#0f1117' }}>
      <style>{`@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}`}</style>

      {/* Header */}
      <div style={{ background: '#1a1d2e', borderBottom: '1px solid #2d3148', padding: '14px 24px', display: 'flex', alignItems: 'center', gap: 12 }}>
        <span style={{ width: 8, height: 8, borderRadius: '50%', background: dotColor, boxShadow: `0 0 8px ${dotColor}`, display: 'inline-block', animation: 'pulse 2s infinite' }} />
        <span style={{ fontWeight: 600, fontSize: 17, color: '#fff' }}>Scraper Monitor</span>
        <span style={{ marginLeft: 'auto', fontSize: 12, color: '#64748b' }}>
          {apiStatus === 'error' ? '⚠ API unreachable — run: uvicorn scraper_api:app --port 8765' : lastUpdate ? `Updated ${lastUpdate.toLocaleTimeString()}` : 'Connecting...'}
        </span>
        <button onClick={poll} style={{ marginLeft: 10, background: '#2d3148', border: 'none', color: '#94a3b8', borderRadius: 6, padding: '5px 12px', cursor: 'pointer', fontSize: 12 }}>↺ Refresh</button>
      </div>

      {/* Summary */}
      <div style={{ display: 'flex', gap: 12, padding: '20px 24px', flexWrap: 'wrap' }}>
        {[
          { label: 'Branches', value: reports.length, color: '#fff' },
          { label: 'Products', value: fmt(total), color: '#fff' },
          { label: 'Price Changes', value: fmt(prices), color: '#fff' },
          { label: 'Specials', value: fmt(specials), color: '#fbbf24' },
          { label: 'Out of Stock', value: fmt(oos), color: '#f87171' },
          { label: 'Cat Failures', value: catFails, color: catFails > 0 ? '#ef4444' : '#22c55e' },
        ].map(s => (
          <div key={s.label} style={{ background: '#1a1d2e', border: '1px solid #2d3148', borderRadius: 8, padding: '14px 20px', minWidth: 130 }}>
            <div style={{ fontSize: 11, color: '#64748b', textTransform: 'uppercase', letterSpacing: '.05em', marginBottom: 6 }}>{s.label}</div>
            <div style={{ fontSize: 26, fontWeight: 700, color: s.color }}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Filters */}
      <div style={{ display: 'flex', gap: 8, padding: '0 24px 16px', flexWrap: 'wrap' }}>
        {CHAINS.map(c => (
          <button key={c} onClick={() => setChain(c)} style={{ padding: '6px 14px', borderRadius: 20, border: `1px solid ${chain === c ? '#4f6ef7' : '#2d3148'}`, background: chain === c ? '#4f6ef7' : 'transparent', color: chain === c ? '#fff' : '#94a3b8', cursor: 'pointer', fontSize: 13 }}>
            {c}
          </button>
        ))}
      </div>

      {/* Reports */}
      <div style={{ padding: '0 24px 40px', display: 'flex', flexDirection: 'column', gap: 10 }}>
        {sorted.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '60px 20px', color: '#475569' }}>
            <div style={{ fontSize: 40 }}>📡</div>
            <p style={{ marginTop: 10, fontSize: 13 }}>No reports yet — start a scraper and results will appear here automatically every 5s.</p>
          </div>
        ) : (
          sorted.map((r, i) => <BranchCard key={`${r.branch_id}-${i}`} report={r} />)
        )}
      </div>
    </div>
  )
}
