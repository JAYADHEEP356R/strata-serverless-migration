import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Activity, ArrowDownToLine, ArrowRight, Bell, Boxes, Check, ChevronDown, ChevronRight, Command, ExternalLink, FileSearch, HelpCircle, LayoutDashboard, Loader2, Menu, Monitor, Moon, RefreshCw, Search, Settings2, ShieldCheck, SlidersHorizontal, Sun, X, Zap } from 'lucide-react';
import type { Asset, Page, Preferences, Report } from './types';
import { download, shortName, timeAgo, visibleReport } from './lib';
import { AssetDrawer, AssetIcon, Badge, Brand, Dialog, ErrorPanel, Skeleton } from './components';
import { ActivityPage, CompatibilityPage, InventoryPage, OverviewPage, ValidationPage } from './pages';
import PlansPage from './PlansPage';

const navigation = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'inventory', label: 'Asset inventory', icon: Boxes },
  { id: 'compatibility', label: 'Compatibility', icon: FileSearch },
  { id: 'plans', label: 'Migration plans', icon: ShieldCheck },
  { id: 'validation', label: 'Validation', icon: ShieldCheck },
  { id: 'activity', label: 'Activity', icon: Activity },
] as const;
const defaults: Preferences = { theme: 'light', fixtures: false, compact: false, autoRefresh: false };
function initialPreferences(): Preferences {
  try {
    const value = JSON.parse(localStorage.getItem('migration-preferences') || '{}');
    return { theme: ['light','dark','system'].includes(value.theme) ? value.theme : 'light', fixtures: value.fixtures === true, compact: value.compact === true, autoRefresh: value.autoRefresh === true };
  } catch { return defaults; }
}
const getPage = (): Page => { const raw = window.location.hash.replace(/^#\/?/, ''); const id = raw === 'workloads' ? 'plans' : raw; return navigation.some(n => n.id === id) ? id as Page : 'overview'; };
export default function App() {
  const [page, setCurrentPage] = useState<Page>(getPage);
  const [report, setReport] = useState<Report | null>(null);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState('');
  const [preferences, setPreferences] = useState(initialPreferences);
  const [settings, setSettings] = useState(false);
  const [help, setHelp] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [mobileNav, setMobileNav] = useState(false);
  const [mobile, setMobile] = useState(() => window.matchMedia('(max-width: 850px)').matches);
  const sidebar = useRef<HTMLElement>(null);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [planAssetKey, setPlanAssetKey] = useState('');
  const [planId, setPlanId] = useState('');
  const [planRequest, setPlanRequest] = useState(0);
  const [notification, setNotification] = useState('');
  const [now, setNow] = useState(Date.now());
  const request = useRef<AbortController | null>(null);
  const toastTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  const toast = useCallback((message: string) => { setNotification(message); if (toastTimeout.current) clearTimeout(toastTimeout.current); toastTimeout.current = setTimeout(() => setNotification(''), 4500); }, []);
  const setPage = (target: Page) => { window.location.hash = '/' + target; setCurrentPage(target); setMobileNav(false); setSelectedKey(null); window.scrollTo({ top: 0, behavior: 'instant' }); };
  const refresh = useCallback(async (manual = false) => {
    request.current?.abort();
    const controller = new AbortController(); request.current = controller;
    const timeout = setTimeout(() => controller.abort('timeout'), 60000);
    setBusy(true); setError('');
    try {
      const response = await fetch('/api/report?refresh=true&request=' + encodeURIComponent(crypto.randomUUID()), { signal: controller.signal, cache: 'no-store' });
      if (!response.ok) throw new Error('The latest assessment could not be read. Check the discovery run and the app’s volume access, then try again.');
      const result = await response.json();
      if (!Array.isArray(result.assets) || !Array.isArray(result.findings) || !Array.isArray(result.relationships)) throw new Error('The saved assessment has an unsupported format. Publish a new snapshot and try again.');
      if (request.current !== controller || controller.signal.aborted) return;
      setReport({ ...result, errors: result.errors || [], validation_evidence: result.validation_evidence || [] });
      if (manual) toast('Latest published assessment loaded');
    } catch (cause) {
      if (controller.signal.aborted && controller.signal.reason !== 'timeout') return;
      setError(controller.signal.reason === 'timeout' ? 'The connection took too long. Please retry; your last loaded assessment is preserved.' : cause instanceof Error ? cause.message : 'Unable to connect to the workspace. Please retry.');
    } finally { clearTimeout(timeout); if (request.current === controller) setBusy(false); }
  }, [toast]);
  useEffect(() => { refresh(); return () => request.current?.abort(); }, [refresh]);
  useEffect(() => {
    const hash = () => { setCurrentPage(getPage()); setMobileNav(false); setSelectedKey(null); };
    const key = (e: KeyboardEvent) => { if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); setSearchOpen(v => !v); } if (e.key === 'Escape') setMobileNav(false); };
    window.addEventListener('hashchange', hash); window.addEventListener('keydown', key);
    const clock = setInterval(() => setNow(Date.now()), 60000);
    return () => { window.removeEventListener('hashchange', hash); window.removeEventListener('keydown', key); clearInterval(clock); if (toastTimeout.current) clearTimeout(toastTimeout.current); };
  }, []);
  useEffect(() => {
    const media = window.matchMedia('(max-width: 850px)');
    const change = () => setMobile(media.matches);
    media.addEventListener('change', change);
    return () => media.removeEventListener('change', change);
  }, []);
  useEffect(() => {
    if (!mobileNav || !mobile) return;
    const root = sidebar.current!;
    const previous = document.activeElement as HTMLElement | null;
    const controls = () => [...root.querySelectorAll<HTMLButtonElement>('button:not(:disabled)')];
    const overflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    controls()[0]?.focus();
    const trap = (e: KeyboardEvent) => {
      if (e.key !== 'Tab') return;
      const elements = controls(); const first = elements[0]; const last = elements.at(-1);
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last?.focus(); }
      if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first?.focus(); }
    };
    root.addEventListener('keydown', trap);
    return () => { root.removeEventListener('keydown', trap); document.body.style.overflow = overflow; previous?.focus({ preventScroll: true }); };
  }, [mobileNav, mobile]);
  useEffect(() => {
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const apply = () => { document.documentElement.dataset.theme = preferences.theme === 'system' ? media.matches ? 'dark' : 'light' : preferences.theme; };
    apply(); media.addEventListener('change', apply);
    document.documentElement.dataset.density = preferences.compact ? 'compact' : 'comfortable';
    try { localStorage.setItem('migration-preferences', JSON.stringify(preferences)); } catch { /* Browser storage is optional. */ }
    return () => media.removeEventListener('change', apply);
  }, [preferences]);
  useEffect(() => {
    if (!preferences.autoRefresh) return;
    const interval = setInterval(() => { if (!document.hidden) refresh(); }, 60000);
    return () => clearInterval(interval);
  }, [preferences.autoRefresh, refresh]);
  const visible = useMemo(() => report ? visibleReport(report, preferences.fixtures) : { assets: [], findings: [], relationships: [] }, [report, preferences.fixtures]);
  const selected = visible.assets.find(a => a.asset_key === selectedKey);
  const searchResults = visible.assets.filter(a => `${a.name} ${a.path || ''} ${a.asset_type}`.toLowerCase().includes(query.toLowerCase())).slice(0, 12);
  const actor = report?.actor || 'Workspace user';
  const initials = actor.split('@')[0].split(/[._\s]+/).filter(Boolean).map(x => x[0]).slice(0, 2).join('').toUpperCase() || 'WS';
  const fixtureCount = report?.assets.filter(a => a.is_fixture).length || 0;
  const current = navigation.find(n => n.id === page)!;
  const exportReport = () => { if (!report) return; download(`migration-assessment-${report.scan_id}.json`, JSON.stringify({ ...report, ...visible, export_scope: preferences.fixtures ? 'including_fixtures' : 'excluding_fixtures' }, null, 2)); toast('Assessment exported'); };
  const pageProps = report ? { report, ...visible, onAsset: (a: Asset) => setSelectedKey(a.asset_key), navigate: setPage, toast } : null;
  return <div className="app-shell" data-clock={now}>
    <a className="skip-link" href="#main-content" onClick={e => { e.preventDefault(); document.getElementById('main-content')?.focus(); }}>Skip to content</a>
    {mobileNav && <button className="sidebar-scrim" aria-label="Dismiss navigation overlay" onClick={() => setMobileNav(false)} />}
    <aside ref={sidebar} inert={mobile && !mobileNav} className={`sidebar ${mobileNav ? 'sidebar-open' : ''}`} aria-label="Main navigation"><div className="sidebar-top"><Brand /><button className="icon-button mobile-close" aria-label="Close navigation" onClick={() => setMobileNav(false)}><X size={19} /></button></div><span className="brand-tagline">MIGRATION CONTROL PLANE</span>
      <button className="workspace-switch" onClick={() => setHelp(true)} title="View workspace scope"><span className="workspace-icon">D</span><span><strong>Databricks workspace</strong><small>{report?.scope === 'workspace' ? 'Visible workspace assets' : 'Project environment'}</small></span><ChevronDown size={15} /></button>
      <span className="nav-group-label">WORKSPACE</span><nav>{navigation.map(n => <button key={n.id} onClick={() => setPage(n.id)} className={`nav-item ${page === n.id ? 'active' : ''}`} aria-current={page === n.id ? 'page' : undefined}><n.icon size={18} strokeWidth={1.7} /><span>{n.label}</span>{n.id === 'inventory' && report && <span className="nav-count">{visible.assets.length}</span>}{n.id === 'compatibility' && visible.findings.length > 0 && <span className="nav-count">{visible.findings.length}</span>}</button>)}</nav>
      <div className="sidebar-lower"><div className="safety-card"><span className="safety-symbol"><ShieldCheck size={20} /></span><strong>Explore with confidence</strong><p>Assessment mode keeps your existing workloads untouched.</p><span><i /> Read-only workload access</span></div><button className="nav-item" onClick={() => setHelp(true)}><HelpCircle size={18} /><span>Workspace & help</span><ArrowRight size={15} /></button><button className="nav-item" onClick={() => setSettings(true)}><Settings2 size={18} /><span>Preferences</span></button><div className="sidebar-user"><span className="avatar">{initials}</span><div><strong>{actor.split('@')[0].replaceAll('.', ' ')}</strong><small>Workspace operator</small></div><span className="user-online" title="Assessment workspace" /></div></div>
    </aside>
    <div className="main-shell" inert={mobile && mobileNav}><header className="topbar"><div className="breadcrumb"><button className="icon-button menu-trigger" aria-label="Open navigation" onClick={() => setMobileNav(true)}><Menu size={20} /></button><span className="breadcrumb-parent">Workspace</span><ChevronRight size={14} /><strong>{current.label}</strong></div><div className="topbar-actions"><button className="global-search" onClick={() => { setSearchOpen(true); setQuery(''); }} aria-label="Search anything… Ctrl K"><Search size={16} /><span>Search anything…</span><kbd>Ctrl K</kbd></button><span className="topbar-divider" /><button className="icon-button" aria-label="View recent activity" onClick={() => setPage('activity')}><Bell size={19} />{report?.validation_evidence?.length ? <i className="notification-dot" /> : null}</button><button className="topbar-avatar" aria-label={`Open preferences, ${initials}`} onClick={() => setSettings(true)}>{initials}</button></div></header>
      <main id="main-content" tabIndex={-1}><div className="page-topline"><div className="page-context"><span className={`live-dot ${error ? 'live-dot-warning' : ''}`} />{error && report ? 'Showing saved assessment' : report ? 'Workspace connected' : busy ? 'Connecting to workspace' : 'Connection unavailable'}<span className="context-divider">/</span><span>{report?.scope === 'workspace' ? 'Visible workspace scope' : 'Project scope'}</span>{preferences.fixtures && <Badge tone="amber">Fixtures included</Badge>}</div><div className="page-top-actions"><span className="snapshot-age" title={report?.scan_time}>Last scan {report ? timeAgo(report.scan_time) : '—'}</span><button className="button button-small" disabled={busy} onClick={() => refresh(true)} title="Reload the published snapshot. This does not start compute.">{busy ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />}<span>Refresh data</span></button></div></div>
        {error && report && <div className="notice notice-warning" role="alert"><span>{error} Showing the last loaded assessment.</span><button onClick={() => refresh(true)}>Retry</button></div>}
        {preferences.fixtures && <div className="notice notice-fixture"><FileSearch size={16} /><span>Assessment fixtures are included. These examples have not been executed and are not migrated workloads.</span><button onClick={() => setPreferences(p => ({ ...p, fixtures: false }))}>Hide fixtures</button></div>}
        {!report ? busy ? <Skeleton /> : <ErrorPanel retry={() => refresh(true)} busy={busy} message={error} /> : pageProps && <div className="page-enter" key={page}>{page === 'overview' && <OverviewPage {...pageProps} onExport={exportReport} />}{page === 'inventory' && <InventoryPage {...pageProps} />}{page === 'compatibility' && <CompatibilityPage {...pageProps} />}{page === 'plans' && <PlansPage initialRequest={planRequest} reload={() => refresh(true)} onAsset={pageProps.onAsset} report={report} assets={visible.assets} initialAssetKey={planAssetKey} initialPlanId={planId} toast={toast} />}{page === 'validation' && <ValidationPage {...pageProps} />}{page === 'activity' && <ActivityPage {...pageProps} onExport={exportReport} />}</div>}
        <footer className="page-footer"><span><ShieldCheck size={13} />Discover, plan & validate · Existing workloads protected</span><span>Strata <i /> Migration intelligence</span></footer>
      </main>
    </div>
    {notification && <div className="toast" role="status"><span><Check size={15} /></span>{notification}<button className="icon-button" aria-label="Dismiss notification" onClick={() => setNotification('')}><X size={15} /></button></div>}
    {selected && report && <AssetDrawer asset={selected} report={report} onClose={() => setSelectedKey(null)} onSelect={a => setSelectedKey(a.asset_key)} onPlan={asset => { setPlanRequest(value => value + 1); setPlanId(''); setPlanAssetKey(asset.asset_key); setSelectedKey(null); setPage('plans'); }} toast={toast} />}
    {searchOpen && <Dialog title="Search your workspace" onClose={() => setSearchOpen(false)} className="search-dialog"><div className="command-input"><Search size={20} /><input autoFocus aria-label="Search all assets" placeholder="Find a job, notebook, task, or cluster…" value={query} onChange={e => setQuery(e.target.value)} /><kbd>esc</kbd></div><div className="command-results"><span className="eyebrow">{query ? `${searchResults.length} matching assets` : 'PROJECT ASSETS'}</span>{searchResults.map(a => <button className="command-result" key={a.asset_key} onClick={() => { setSearchOpen(false); setSelectedKey(a.asset_key); }}><AssetIcon asset={a} small /><div><strong>{shortName(a)}</strong><small>{a.asset_type.toLowerCase()}{a.is_fixture ? ' · Fixture' : ''}</small></div><ArrowRight size={16} /></button>)}{!searchResults.length && <p className="search-empty">No assets match “{query}”. Try a different name.</p>}</div><div className="command-footer"><Command size={13} /> Search covers the selected scope{!preferences.fixtures && fixtureCount > 0 ? ' · fixtures hidden' : ''}</div></Dialog>}
    {settings && <Dialog title="Make it your workspace" onClose={() => setSettings(false)} className="preferences-dialog"><p className="dialog-description">A few preferences to fit the way you work. Saved in this browser.</p><div className="preference-section"><span className="setting-label">Appearance</span><div className="theme-options">{([{ id: 'light', label: 'Light', icon: Sun }, { id: 'dark', label: 'Dark', icon: Moon }, { id: 'system', label: 'System', icon: Monitor }] as const).map(t => <button className={preferences.theme === t.id ? 'selected' : ''} aria-pressed={preferences.theme === t.id} key={t.id} onClick={() => setPreferences(p => ({ ...p, theme: t.id }))}><t.icon size={20} /><span>{t.label}</span>{preferences.theme === t.id && <Check size={13} />}</button>)}</div></div>{[{ key: 'compact', title: 'Compact tables', description: 'Fit more assets into your inventory view.', icon: SlidersHorizontal }, { key: 'autoRefresh', title: 'Keep the snapshot up to date', description: 'Check for published data every minute while this tab is visible.', icon: RefreshCw }, { key: 'fixtures', title: 'Include assessment fixtures', description: `Show ${fixtureCount} static test examples alongside project assets.`, icon: FileSearch }].map(option => <label className="preference-row" key={option.key}><option.icon size={19} /><span><strong>{option.title}</strong><small>{option.description}</small></span><input type="checkbox" className="switch" checked={Boolean(preferences[option.key as keyof Preferences])} onChange={e => setPreferences(p => ({ ...p, [option.key]: e.target.checked }))} /></label>)}<button className="button button-primary preferences-done" onClick={() => setSettings(false)}>Done<Check size={16} /></button></Dialog>}
    {help && <Dialog title="Your assessment workspace" onClose={() => setHelp(false)}><div className="help-banner"><ShieldCheck size={25} /><div><strong>Connected to your project, safely.</strong><p>This app discovers accessible workspace assets through a dedicated scanner, prepares selected plans, and records decisions in project storage. It does not modify or run discovered workloads.</p></div></div><dl className="detail-list"><dt>Scope</dt><dd>{report?.scope === 'workspace' ? 'Assets visible to the scanner operator' : 'Dedicated project assets'}</dd><dt>Snapshot</dt><dd className="mono">{report?.scan_id || 'Not available'}</dd><dt>Rule version</dt><dd className="mono">{report?.rule_version || 'Not recorded'}</dd><dt>Storage</dt><dd>{report?.project_schema || 'Project artifacts volume'}</dd></dl><div className="help-section"><h3>What is available today?</h3><p>Discover assets, inspect compatibility, generate candidate-only plans, record project-operator decisions, and review synthetic validation evidence.</p><h3>What happens next?</h3><p>The isolated demo execution trial requires an approved current plan and separate run confirmation. Validation creates isolated classic and serverless copies, compares their outputs, and records the result with direct job and run links. Execution for other workloads, scheduled cutover, data rollback, and workspace-admin approval integration remain upcoming stages. Static assessment does not prove runtime compatibility.</p><h3>About cost reporting</h3><p>Billing access is unavailable. Cost and savings figures will appear only when supported by actual billing evidence.</p></div><div className="dialog-actions"><button className="button" onClick={() => { setHelp(false); setPage('activity'); }}>View coverage<ArrowRight size={15} /></button><a className="button button-text" href="https://docs.databricks.com/aws/en/compute/serverless/limitations" target="_blank" rel="noreferrer">Databricks documentation<ExternalLink size={14} /></a></div></Dialog>}
  </div>;
}
