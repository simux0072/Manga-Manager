import {useEffect, useMemo, useRef, useState} from 'react'
import {useInfiniteQuery, useMutation, useQuery, useQueryClient} from '@tanstack/react-query'
import {AlertTriangle, BookOpen, Check, ExternalLink, Merge, Search, Split, X} from 'lucide-react'

import {api} from './api'
import type {MatchSide, MergeCandidate, MergePreview, Series} from './types'

const fallbackProviders = ['asura', 'mangafire', 'kingofshojo']

export function MatchesWorkspace() {
  const [tab, setTab] = useState<'suggested' | 'manual'>('suggested')
  const providerQuery = useQuery({queryKey: ['providers'], queryFn: api.providers, staleTime: 300_000})
  const providers = providerQuery.data?.items?.length ? providerQuery.data.items : fallbackProviders
  return <>
    <header className="page-header">
      <div>
        <h1>Match manga</h1>
        <p>Review likely matches or combine two or more provider-compatible library entries.</p>
      </div>
    </header>
    <nav className="workspace-tabs" aria-label="Match workspace">
      <button className={tab === 'suggested' ? 'active' : ''} onClick={() => setTab('suggested')}>Suggested</button>
      <button className={tab === 'manual' ? 'active' : ''} onClick={() => setTab('manual')}>Manual merge</button>
    </nav>
    {tab === 'suggested' ? <SuggestedMatches /> : <ManualMerge providers={providers} />}
  </>
}

function SuggestedMatches() {
  const client = useQueryClient()
  const [confirm, setConfirm] = useState<number | null>(null)
  const [selected, setSelected] = useState<number[]>([])
  const [entireQueue, setEntireQueue] = useState(false)
  const [batchPreview, setBatchPreview] = useState<{selected:number;eligible:number;blocked:number;items:{id:number;blocked_reasons:string[]}[]}|null>(null)
  const [confirmBatch, setConfirmBatch] = useState(false)
  const query = useInfiniteQuery({
    queryKey: ['matches'],
    queryFn: ({pageParam}) => api.matches(pageParam),
    initialPageParam: 0,
    getNextPageParam: page => page.next_cursor || undefined,
  })
  const decision = useMutation({
    mutationFn: ({id, value}: {id: number; value: 'accepted' | 'rejected'}) =>
      api.decideMatch(id, value, value === 'accepted' ? 'MERGE' : ''),
    onSuccess: () => {
      setConfirm(null)
      client.invalidateQueries({queryKey: ['matches']})
      client.invalidateQueries({queryKey: ['library']})
    },
  })
  const batch = useMutation({
    mutationFn: ({value}:{value:'accepted'|'rejected'}) => api.decideMatches(selected,value,value==='accepted'?'MERGE':'',entireQueue),
    onSuccess: result => {
      setSelected([]); setEntireQueue(false); setBatchPreview(null); setConfirmBatch(false)
      client.invalidateQueries({queryKey:['matches']}); client.invalidateQueries({queryKey:['library']})
      if(result.blocked.length) window.dispatchEvent(new CustomEvent('manga-toast',{detail:{message:`${result.blocked.length} blocked proposals remain pending`}}))
    },
  })
  const items = query.data?.pages.flatMap(page => page.items) || []
  if (query.isLoading) return <Loading />
  if (query.isError) return <Message icon={<AlertTriangle />} title="Could not load matches" detail={query.error.message} />
  if (!items.length) return <Message icon={<Check />} title="No matches need review" detail="Use Manual merge when you already know two titles belong together." />
  return <>
    <section className="match-batch-bar">
      <label><input type="checkbox" checked={entireQueue} onChange={async event=>{const value=event.target.checked;setEntireQueue(value);setSelected(value?[]:selected);setBatchPreview(await api.previewMatches(value?[]:selected,value))}}/> Select entire queue</label>
      <span>{entireQueue?'Entire queue':`${selected.length} selected`}</span>
      <button className="secondary" disabled={!entireQueue&&!selected.length} onClick={async()=>setBatchPreview(await api.previewMatches(selected,entireQueue))}>Preview</button>
      <button className="secondary" disabled={!entireQueue&&!selected.length} onClick={()=>batch.mutate({value:'rejected'})}>Keep separate</button>
      <button className="primary" disabled={!entireQueue&&!selected.length} onClick={async()=>{setBatchPreview(await api.previewMatches(selected,entireQueue));setConfirmBatch(true)}}>Merge eligible</button>
      {batchPreview&&<span>{batchPreview.eligible} eligible · {batchPreview.blocked} blocked</span>}
    </section>
    {batchPreview?.items.some(item=>item.blocked_reasons.length>0)&&<div className="inline-notice" role="status">Blocked proposals remain pending: {batchPreview.items.filter(item=>item.blocked_reasons.length).map(item=>`#${item.id} ${item.blocked_reasons.join(', ')}`).join(' · ')}</div>}
    <div className="match-list">
      {items.map(match => <article className="match-card" key={match.id}>
        <label className="match-select"><input type="checkbox" checked={selected.includes(match.id)} disabled={entireQueue} onChange={()=>setSelected(current=>current.includes(match.id)?current.filter(id=>id!==match.id):[...current,match.id])}/>Select</label>
        <Side side={match.left} />
        <div className="match-evidence">
          <div className="confidence"><b>{Math.round(match.confidence * 100)}%</b><span>confidence</span></div>
          {match.evidence.map(item => <span className={`evidence evidence-${item.tone}`} key={item.label}>{item.label}</span>)}
          {match.blocked_reasons.map(reason=><span className="evidence evidence-warning" key={reason}>{reason}</span>)}
          <div className="match-actions">
            <button className="secondary" onClick={() => decision.mutate({id: match.id, value: 'rejected'})}><Split />Keep separate</button>
            <button className="primary" onClick={() => setConfirm(match.id)}><Merge />Merge</button>
          </div>
        </div>
        <Side side={match.right} />
      </article>)}
    </div>
    <Pager hasNext={!!query.hasNextPage} loading={query.isFetchingNextPage} load={() => query.fetchNextPage()} />
    {confirm !== null && <ConfirmModal
      title="Merge these manga?"
      detail="Their providers, chapters, files, and reading state will be consolidated under the best canonical record."
      busy={decision.isPending}
      error={decision.error?.message}
      close={() => setConfirm(null)}
      confirm={() => decision.mutate({id: confirm, value: 'accepted'})}
    />}
    {confirmBatch && batchPreview && <ConfirmModal
      title={`Merge ${batchPreview.eligible} eligible proposal${batchPreview.eligible===1?'':'s'}?`}
      detail={`${batchPreview.blocked} blocked proposal${batchPreview.blocked===1?'':'s'} will remain pending. Connected matches are merged once per canonical component.`}
      busy={batch.isPending}
      error={batch.error?.message}
      close={()=>setConfirmBatch(false)}
      confirm={()=>batch.mutate({value:'accepted'})}
      disabled={batchPreview.eligible===0}
    />}
  </>
}

function ManualMerge({providers}: {providers: string[]}) {
  const client = useQueryClient()
  const [queryText, setQueryText] = useState('')
  const [debounced, setDebounced] = useState('')
  const [sources, setSources] = useState<string[]>([])
  const [selected, setSelected] = useState<Series[]>([])
  const [preview, setPreview] = useState<MergePreview | null>(null)
  const [notice, setNotice] = useState('')

  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(queryText.trim()), 220)
    return () => window.clearTimeout(timer)
  }, [queryText])

  const library = useInfiniteQuery({
    queryKey: ['merge-library', debounced, sources],
    queryFn: ({pageParam}) => api.library(debounced, sources, ['interested', 'reading', 'caught_up', 'paused'], pageParam),
    initialPageParam: '',
    getNextPageParam: page => page.next_cursor || undefined,
    enabled: selected.length === 0,
  })
  const candidates = useInfiniteQuery({
    queryKey: ['merge-candidates', selected.map(item => item.id), debounced, sources],
    queryFn: ({pageParam}) => api.mergeCandidates(selected.map(item => item.id), debounced, sources, pageParam),
    initialPageParam: 0,
    getNextPageParam: page => page.next_cursor || undefined,
    enabled: selected.length > 0,
  })
  const previewMutation = useMutation({
    mutationFn: () => api.mergePreview(selected.map(item => item.id)),
    onSuccess: setPreview,
    onError: error => setNotice(error.message),
  })
  const mergeMutation = useMutation({
    mutationFn: () => api.mergeSeries(selected.map(item => item.id)),
    onSuccess: result => {
      setPreview(null)
      setSelected([])
      setNotice(`Merged into series #${result.target_id}. Library repair has been queued.`)
      client.invalidateQueries({queryKey: ['library']})
      client.invalidateQueries({queryKey: ['matches']})
    },
  })

  const raw = selected.length
    ? candidates.data?.pages.flatMap(page => page.items) || []
    : library.data?.pages.flatMap(page => page.items) || []
  const selectedIds = new Set(selected.map(item => item.id))
  const selectedProviders = new Set(selected.flatMap(item => item.sources.map(source => source.name)))
  const items = useMemo(() => raw.filter(item => !selectedIds.has(item.id)), [raw, selectedIds])
  const loading = selected.length ? candidates.isLoading : library.isLoading
  const error = selected.length ? candidates.error : library.error
  const hasNext = selected.length ? candidates.hasNextPage : library.hasNextPage
  const fetching = selected.length ? candidates.isFetchingNextPage : library.isFetchingNextPage
  const fetchNext = selected.length ? candidates.fetchNextPage : library.fetchNextPage

  const choose = (item: Series | MergeCandidate) => {
    if (selected.length >= providers.length) return
    const conflicts = item.sources.map(source => source.name).filter(source => selectedProviders.has(source))
    if ('compatible' in item && !item.compatible) {
      setNotice(`This candidate conflicts on ${item.conflicting_sources.join(', ')}.`)
      return
    }
    const blockingConflicts = conflicts
    if (blockingConflicts.length) {
      setNotice(`Cannot select another ${blockingConflicts.join(', ')} identity. A canonical manga can contain only one unresolved identity per provider.`)
      return
    }
    setNotice('')
    setSelected(current => [...current, item])
  }
  const remove = (id: number) => {
    setPreview(null)
    setSelected(current => current.filter(item => item.id !== id))
  }
  const toggleSource = (source: string) => setSources(current =>
    current.includes(source) ? current.filter(item => item !== source) : [...current, source],
  )

  return <>
    <section className="merge-selection">
      <div><b>Merge selection</b><span>Choose 2–{providers.length} records from different providers.</span></div>
      <div className="merge-slots">
        {providers.map((_provider, index) => selected[index]
          ? <button key={selected[index].id} onClick={() => remove(selected[index].id)} title="Remove from selection">
              <Cover series={selected[index]} /><span>{selected[index].title}</span><X />
            </button>
          : <div className="empty-slot" key={index}>Selection {index + 1}</div>)}
      </div>
      <button className="primary" disabled={selected.length < 2 || previewMutation.isPending} onClick={() => previewMutation.mutate()}>
        <Merge />Review merge
      </button>
    </section>
    {notice && <div className="inline-notice" role="status">{notice}</div>}
    <section className="filter-stack">
      <label className="search-field"><Search /><input value={queryText} onChange={event => setQueryText(event.target.value)} placeholder="Search tracked manga…" /></label>
      <div className="chip-group">{providers.map(source => <button key={source} className={`filter-chip ${sources.includes(source) ? 'selected' : ''}`} onClick={() => toggleSource(source)}>{source}</button>)}</div>
    </section>
    {selected.length > 0 && <p className="ranking-note">Candidates are ranked by their strongest title, cover, description, and chapter evidence across every selected manga.</p>}
    {loading ? <Loading /> : error ? <Message icon={<AlertTriangle />} title="Could not load library" detail={error.message} /> : items.length ? <>
      <div className="manual-merge-grid">
        {items.map(item => {
          const candidate = isMergeCandidate(item) ? item : null
          return <button className={`merge-candidate ${candidate && !candidate.compatible ? 'incompatible' : ''}`} disabled={!!candidate && !candidate.compatible} onClick={() => choose(item)} key={item.id}>
            <Cover series={item} />
            <div><h2>{item.title}</h2><div className="source-row">{item.sources.map(source => <span key={source.name}>{source.name}</span>)}</div><p>{item.description || 'No description available.'}</p><span className="candidate-meta">Latest {item.latest_chapter || 'unknown'} · {item.chapter_count} chapters · {item.status.replace('_', ' ')}</span>{candidate && <b>{Math.round(candidate.similarity * 100)}% similar</b>}{candidate?.score_breakdown && <small>Title {Math.round(candidate.score_breakdown.title * 100)}% · Cover {Math.round(candidate.score_breakdown.cover * 100)}% · Description {Math.round(candidate.score_breakdown.description * 100)}% · Chapters {Math.round(candidate.score_breakdown.chapter_overlap * 100)}%</small>}{candidate && !candidate.compatible && <small>Provider conflict: {candidate.conflicting_sources.join(', ')}</small>}</div>
          </button>
        })}
      </div>
      <Pager hasNext={!!hasNext} loading={fetching} load={() => fetchNext()} />
    </> : <Message icon={<Search />} title="No compatible manga found" detail="Try another title or remove a provider filter." />}
    {preview && <ConfirmModal
      title={`Merge into ${preview.target_title}?`}
      detail={preview.can_merge ? `${preview.items.length} records will become one canonical manga. The best source is selected automatically.` : `Provider conflict: ${preview.conflicting_sources.join(', ')}`}
      busy={mergeMutation.isPending}
      error={mergeMutation.error?.message}
      close={() => setPreview(null)}
      confirm={() => mergeMutation.mutate()}
      disabled={!preview.can_merge}
    />}
  </>
}

function Side({side}: {side: MatchSide}) {
  return <section className="match-side"><Cover series={side} /><div><span className="source-badge">{side.source}</span><h2>{side.source_title}</h2><p>{side.description || 'No description available.'}</p><div className="match-meta"><span>Latest {side.latest_chapter || 'unknown'}</span><a href={side.url} target="_blank" rel="noreferrer">Open source <ExternalLink /></a></div></div></section>
}

function Cover({series}: {series: Pick<Series, 'title' | 'cover_url'>}) {
  const [failed, setFailed] = useState(false)
  return <div className="cover">{series.cover_url && !failed ? <img src={series.cover_url} alt={`Cover for ${series.title}`} loading="lazy" onError={() => setFailed(true)} /> : <div className="cover-placeholder"><BookOpen /><span>Cover unavailable</span></div>}</div>
}

function ConfirmModal({title, detail, busy, error, close, confirm, disabled = false}: {title: string; detail: string; busy: boolean; error?: string; close: () => void; confirm: () => void; disabled?: boolean}) {
  return <div className="modal-backdrop" onMouseDown={close}><div className="modal" role="dialog" aria-modal="true" onMouseDown={event => event.stopPropagation()}><div className="modal-icon"><AlertTriangle /></div><h2>{title}</h2><p>{detail}</p>{error && <div className="inline-notice error">{error}</div>}<div className="modal-actions"><button className="secondary" onClick={close}>Cancel</button><button className="danger" disabled={busy || disabled} onClick={confirm}>{busy ? 'Merging…' : 'Confirm merge'}</button></div></div></div>
}

function Pager({hasNext, loading, load}: {hasNext: boolean; loading: boolean; load: () => void}) {
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const node = ref.current
    if (!node || !hasNext) return
    const observer = new IntersectionObserver(entries => {
      if (entries[0]?.isIntersecting && !loading) load()
    }, {rootMargin: '600px'})
    observer.observe(node)
    return () => observer.disconnect()
  }, [hasNext, loading, load])
  return hasNext ? <div ref={ref} className="auto-pager"><button className="load-more" disabled={loading} onClick={load}>{loading ? 'Loading…' : 'Load more'}</button></div> : null
}

function Loading() {
  return <div className="panel-loading" aria-label="Loading" />
}

function Message({icon, title, detail}: {icon: React.ReactNode; title: string; detail: string}) {
  return <div className="empty-state">{icon}<h2>{title}</h2><p>{detail}</p></div>
}

function isMergeCandidate(item: Series | MergeCandidate): item is MergeCandidate {
  return 'similarity' in item && 'compatible' in item
}
