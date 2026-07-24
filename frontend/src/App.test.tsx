import {QueryClient, QueryClientProvider} from '@tanstack/react-query'
import {act, fireEvent, render, screen, waitFor} from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {MemoryRouter} from 'react-router-dom'
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest'
import {App} from './App'

class EventSourceMock {
  addEventListener = vi.fn()
  close = vi.fn()
}

const series = {
  id: 7,
  title: 'The Painter Who Draws Dungeons',
  description: 'A painter explores impossible dungeons.',
  cover_url: 'https://images.test/cover.jpg',
  status: 'untracked',
  integrity_state: 'healthy',
  latest_chapter: '1',
  latest_source: 'asura',
  latest_at: '2026-07-11T12:00:00Z',
  sources: [{name: 'asura', title: 'Painter', url: 'https://asura.test/painter'}],
  aliases: [],
  chapter_count: 1,
  read_count: 0,
  unread_count: 1,
}
const matchingSeries = {
  ...series,
  id: 9,
  title: 'Dungeon Painter',
  status: 'interested',
  sources: [{name: 'mangafire', title: 'Dungeon Painter', url: 'https://mangafire.test/painter'}],
}

function response(value:unknown){return Promise.resolve(new Response(JSON.stringify(value),{status:200,headers:{'Content-Type':'application/json'}}))}
function renderApp(path='/discovery'){
  const client=new QueryClient({defaultOptions:{queries:{retry:false},mutations:{retry:false}}})
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[path]}><App/></MemoryRouter></QueryClientProvider>)
}

describe('media library frontend',()=>{
  beforeEach(()=>{
    vi.stubGlobal('EventSource',EventSourceMock)
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:1,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/workload-cycle'))return response({id:1,status:'active',total:10,successful:6,failed:0,cancelled:0,superseded:2,remaining:2,added:10})
      if(url.includes('/api/v2/discovery'))return response({items:[series],next_cursor:null})
      if(url.includes('/api/v2/jobs'))return response({items:[],next_cursor:null})
      return response({items:[],next_cursor:null})
    }))
  })
  afterEach(()=>vi.unstubAllGlobals())

  it('searches while typing and applies multiple sources immediately',async()=>{
    renderApp()
    expect(await screen.findByText(series.title)).toBeInTheDocument()
    await userEvent.type(screen.getByLabelText('Search catalog'),'painter')
    await userEvent.click(screen.getByRole('button',{name:/asura/i}))
    await userEvent.click(screen.getByRole('button',{name:/mangafire/i}))
    await waitFor(()=>expect(fetch).toHaveBeenCalledWith(expect.stringContaining('q=painter'),expect.anything()))
    expect(String((fetch as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0])).toContain('source=mangafire')
  })

  it('opens a structured job center without shifting page content',async()=>{
    renderApp()
    await screen.findByText(series.title)
    await userEvent.click(screen.getByRole('button',{name:/active/i}))
    expect(screen.getByRole('complementary',{name:'Job center'})).toBeInTheDocument()
    expect(document.documentElement).toHaveClass('drawer-open')
    expect(await screen.findByText('2 duplicates removed')).toBeInTheDocument()
    expect(screen.getByRole('button',{name:'failed'})).toBeInTheDocument()
    expect(screen.getByRole('button',{name:'running'})).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button',{name:'Close jobs'}))
    expect(document.documentElement).not.toHaveClass('drawer-open')
  })

  it('loads failed operations independently from recent successful jobs',async()=>{
    const failedJob={id:77,kind:'kavita_sync',description:'Synchronize Example with Kavita',source:'',pool:'kavita',cycle_id:1,workflow_key:'',group_key:'kavita',status:'failed',queue_position:null,attempt:3,max_attempts:3,error_code:'cover_fetch_failed',error_message:'cover unavailable',available_at:'2026-07-15T00:00:00Z',created_at:'2026-07-15T00:00:00Z',updated_at:'2026-07-15T00:00:00Z',completed_at:'2026-07-15T00:00:00Z',progress:{phase:'',current:0,total:0,unit:'',bytes:0,message:'',updated_at:null,percent:null},context:{}}
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{failed:1},health:{series:1,chapters:1,active_artifacts:0,missing_projections:0,storage_free_bytes:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/jobs')&&url.includes('state=failed'))return response({items:[failedJob],next_cursor:null})
      if(url.includes('/api/v2/jobs'))return response({items:[],next_cursor:null})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/operations')
    expect(await screen.findByText('Synchronize Example with Kavita')).toBeInTheDocument()
    expect(screen.getByText('cover unavailable')).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button',{name:'Dismiss'}))
    expect(fetch).toHaveBeenCalledWith('/api/v2/jobs/77/dismiss',expect.objectContaining({method:'POST'}))
  })

  it('dismisses all unresolved failures from the Job Center',async()=>{
    const failedJob={id:78,kind:'maintenance',description:'Probe storage',source:'',pool:'health',cycle_id:1,workflow_key:'',group_key:'probe',status:'failed',queue_position:null,attempt:3,max_attempts:3,error_code:'probe_failed',error_message:'disk unavailable',available_at:'2026-07-15T00:00:00Z',created_at:'2026-07-15T00:00:00Z',updated_at:'2026-07-15T00:00:00Z',completed_at:'2026-07-15T00:00:00Z',progress:{phase:'',current:0,total:0,unit:'',bytes:0,message:'',updated_at:null,percent:null},context:{}}
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{failed:1},active_groups:0,health:{series:1,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/workload-cycle'))return response({id:1,status:'settled',total:1,successful:0,failed:1,cancelled:0,superseded:0,remaining:0,added:1})
      if(url.includes('/api/v2/discovery'))return response({items:[series],next_cursor:null})
      if(url.includes('/api/v2/job-groups')&&url.includes('state=failed'))return response({items:[{key:'probe',kind:'maintenance',source:'',title:'Probe storage',cover_url:'',task_count:1,status_counts:{failed:1},progress:{current:1,total:1,percent:100,successful:0,failed:1,cancelled:0},representative:failedJob,single:true}],next_cursor:null})
      if(url.endsWith('/api/v2/jobs/failures/dismiss'))return response({dismissed:1})
      if(url.includes('/api/v2/job-groups'))return response({items:[],next_cursor:null})
      if(url.includes('/api/v2/jobs'))return response({items:[],next_cursor:null})
      return response({items:[],next_cursor:null})
    }))
    renderApp()
    await screen.findByText(series.title)
    await userEvent.click(screen.getByRole('button',{name:/active/i}))
    await userEvent.click(screen.getByRole('button',{name:'failed'}))
    await userEvent.click(await screen.findByRole('button',{name:/Clear failures/i}))
    expect(fetch).toHaveBeenCalledWith('/api/v2/jobs/failures/dismiss',expect.objectContaining({method:'POST'}))
  })

  it('shows whether a provider poll reached its saved frontier',async()=>{
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({
        job_counts:{},active_groups:0,
        health:{series:1,chapters:1,active_artifacts:0,missing_projections:0,storage_free_bytes:0},
        sources:[{source:'mangafire',status:'healthy',failures:0,last_error:'',last_poll_at:'2026-07-20T08:00:00Z',cooldown_until:null,enabled:true,frontier_metrics:{listed:1000,pages_fetched:20,frontier_reached:false,safety_limit_reached:true}}],
        workers:[],permits:{},provider_policies:[],provider_endpoints:[],recent_benchmarks:[],
      })
      if(url.includes('/api/v2/jobs'))return response({items:[],next_cursor:null})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/operations')
    expect(await screen.findByText('Window limit · 20 pages / 1000 titles')).toBeInTheDocument()
  })

  it('previews and confirms a manual cross-provider merge',async()=>{
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request,init?:RequestInit)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/merge-candidates'))return response({items:[{...matchingSeries,similarity:.93,compatible:true,conflicting_sources:[]}],next_cursor:null})
      if(url.includes('/api/v2/library'))return response({items:[{...series,status:'interested'}],next_cursor:null})
      if(url.endsWith('/api/v2/series/merge-preview'))return response({target_id:7,target_title:series.title,items:[series,matchingSeries],conflicting_sources:[],can_merge:true})
      if(url.endsWith('/api/v2/series/merge')&&init?.method==='POST')return response({target_id:7,merged_ids:[7,9]})
      if(url.includes('/api/v2/matches'))return response({items:[],next_cursor:null})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/matches')
    await userEvent.click(await screen.findByRole('button',{name:'Manual merge'}))
    await userEvent.click(await screen.findByRole('button',{name:new RegExp(series.title)}))
    await userEvent.click(await screen.findByRole('button',{name:/Dungeon Painter/}))
    await userEvent.click(screen.getByRole('button',{name:'Review merge'}))
    expect(await screen.findByText(`Merge into ${series.title}?`)).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button',{name:'Confirm merge'}))
    expect(await screen.findByText(/Merged into series #7/)).toBeInTheDocument()
  })

  it('reviews deep suggestions without resetting or refetching loaded pages',async()=>{
    const match={
      id:41,decision_ids:[41],confidence:.92,evidence:[],blocked_reasons:[],
      left:{...series,source_title:series.title,source:'asura',url:'https://asura.test/painter',cover_evidence_used:true},
      right:{...matchingSeries,source_title:matchingSeries.title,source:'mangafire',url:'https://mangafire.test/painter',cover_evidence_used:true},
    }
    const secondMatch={
      ...match,id:42,decision_ids:[42],confidence:.88,
      left:{...match.left,id:11,title:'Another Hero'},
      right:{...match.right,id:12,title:'The Other Hero'},
    }
    const duplicateMatch={...match,id:43,decision_ids:[43],confidence:.90}
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request,init?:RequestInit)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/providers'))return response({items:['asura','mangadex','mangafire','kingofshojo']})
      if(url.includes('/api/v2/matches/')&&init?.method==='POST')return response({id:Number(url.split('/').at(-1)),decision:'reviewed'})
      if(url.includes('/api/v2/matches'))return response({items:[match,duplicateMatch,secondMatch],next_cursor:null,total:3})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/matches')
    await waitFor(()=>expect(document.querySelectorAll('.match-card')).toHaveLength(2))
    expect(screen.getAllByText('Cover used for comparison')).toHaveLength(4)
    const keepSeparate=(await screen.findAllByRole('button',{name:'Keep separate'})).find(button=>!button.hasAttribute('disabled'))!
    const requestsBefore=(fetch as ReturnType<typeof vi.fn>).mock.calls.filter(([input,init])=>
      String(input).includes('/api/v2/matches?')&&(!init||!(init as RequestInit).method),
    ).length
    await userEvent.click(keepSeparate)
    await waitFor(()=>expect(document.querySelectorAll('.match-card')).toHaveLength(1))
    expect(screen.getByText(`Kept ${series.title} and ${matchingSeries.title} separate`)).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button',{name:'Merge'}))
    await userEvent.click(await screen.findByRole('button',{name:'Confirm merge'}))
    await waitFor(()=>expect(document.querySelectorAll('.match-card')).toHaveLength(0))
    expect(screen.getByText('Merged Another Hero and The Other Hero')).toBeInTheDocument()
    const requestsAfter=(fetch as ReturnType<typeof vi.fn>).mock.calls.filter(([input,init])=>
      String(input).includes('/api/v2/matches?')&&(!init||!(init as RequestInit).method),
    ).length
    expect(requestsAfter).toBe(requestsBefore)
  })

  it('deduplicates match pairs and previews only the explicit batch selection',async()=>{
    const match={
      id:51,decision_ids:[51],confidence:.91,evidence:[],blocked_reasons:[],
      left:{...series,source_title:series.title,source:'asura',url:'https://asura.test/painter',cover_evidence_used:true},
      right:{...matchingSeries,source_title:matchingSeries.title,source:'mangafire',url:'https://mangafire.test/painter',cover_evidence_used:true},
    }
    const duplicate={...match,id:52,decision_ids:[52],confidence:.89}
    const other={
      ...match,id:53,decision_ids:[53],confidence:.80,
      left:{...match.left,id:21,title:'Another Left'},
      right:{...match.right,id:22,title:'Another Right'},
    }
    let previewBody:{ids:number[];entire_queue:boolean}|null=null
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request,init?:RequestInit)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/providers'))return response({items:['asura','mangadex','mangafire','kingofshojo']})
      if(url.endsWith('/api/v2/match-batch/preview')){
        previewBody=JSON.parse(String(init?.body))
        return response({selected:previewBody!.ids.length,eligible:previewBody!.ids.length,blocked:0,items:[]})
      }
      if(url.includes('/api/v2/matches'))return response({items:[match,duplicate,other],next_cursor:null,total:3})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/matches')
    await waitFor(()=>expect(document.querySelectorAll('.match-card')).toHaveLength(2))
    const checkboxes=screen.getAllByRole('checkbox')
    await userEvent.click(checkboxes[1])
    await userEvent.click(checkboxes[2])
    await userEvent.click(screen.getByRole('button',{name:'Merge eligible'}))
    expect(await screen.findByText('Merge 2 eligible proposals?')).toBeInTheDocument()
    expect(previewBody).toEqual({ids:[51,53],entire_queue:false,decision:'rejected'})
  })
})
