import {QueryClient, QueryClientProvider} from '@tanstack/react-query'
import {act, fireEvent, render, screen, waitFor} from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {MemoryRouter} from 'react-router-dom'
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest'
import {App} from './App'

class EventSourceMock {
  static instances:EventSourceMock[]=[]
  listeners=new Map<string,((event:Event)=>void)[]>()
  constructor(){EventSourceMock.instances.push(this)}
  addEventListener = vi.fn((type:string,listener:(event:Event)=>void)=>{
    this.listeners.set(type,[...(this.listeners.get(type)||[]),listener])
  })
  close = vi.fn()
  emit(type:string,event:Event){this.listeners.get(type)?.forEach(listener=>listener(event))}
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
    EventSourceMock.instances=[]
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

  it('coalesces a burst of job events into one operations refresh',async()=>{
    renderApp()
    await screen.findByText(series.title)
    const requestMock=fetch as ReturnType<typeof vi.fn>
    const operationRequests=()=>requestMock.mock.calls.filter(call=>String(call[0]).includes('/api/v2/operations')).length
    const before=operationRequests()
    const stream=EventSourceMock.instances.at(-1)
    expect(stream).toBeDefined()

    act(()=>{
      for(let eventId=1;eventId<=100;eventId++){
        stream!.emit('job',new MessageEvent('job',{
          data:JSON.stringify({kind:'source_refresh',type:'succeeded',state:'succeeded'}),
          lastEventId:String(eventId),
        }))
      }
      stream!.emit('counts',new MessageEvent('counts',{data:'{}'}))
    })

    await waitFor(()=>expect(operationRequests()).toBe(before+1))
    await new Promise(resolve=>setTimeout(resolve,350))
    expect(operationRequests()).toBe(before+1)
  })

  it('does not send the application referrer when loading provider covers',async()=>{
    renderApp()
    const cover=await screen.findByRole('img',{name:`Cover for ${series.title}`})
    expect(cover).toHaveAttribute('referrerpolicy','no-referrer')
  })

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

  it('shows retry waits with an amber progress bar and live countdown',async()=>{
    const retryAt=new Date(Date.now()+65_000).toISOString()
    const waitingJob={id:79,kind:'source_pull',description:'Pull MangaDex catalog',source:'mangadex',pool:'pull:mangadex',cycle_id:1,workflow_key:'',group_key:'pull:mangadex',status:'retry_wait',queue_position:null,attempt:1,max_attempts:3,error_code:'source_network_error',error_message:'Provider unavailable',available_at:retryAt,created_at:new Date().toISOString(),updated_at:new Date().toISOString(),completed_at:null,progress:{phase:'catalog',current:1,total:3,unit:'phases',bytes:0,message:'Waiting for provider',updated_at:new Date().toISOString(),percent:33.3},context:{}}
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{retry_wait:1},active_groups:1,health:{series:1,chapters:1,active_artifacts:0,missing_projections:0,storage_free_bytes:0},sources:[],workers:[],permits:{},provider_policies:[],provider_endpoints:[],recent_benchmarks:[]})
      if(url.includes('/api/v2/jobs?')&&url.includes('state=retry_wait'))return response({items:[waitingJob],next_cursor:null})
      if(url.includes('/api/v2/jobs'))return response({items:[],next_cursor:null})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/operations')
    expect(await screen.findByText('Waiting to retry')).toBeInTheDocument()
    expect(screen.getByText(/Retry in 1m/)).toBeInTheDocument()
    expect(document.querySelector('.progress-track.waiting')).toBeInTheDocument()
    const retryNow=screen.getByRole('button',{name:'Retry now'})
    const dismiss=screen.getByRole('button',{name:'Dismiss'})
    expect(retryNow).toHaveClass('retry-now')
    expect(dismiss).toHaveClass('dismiss-wait')
    await userEvent.click(retryNow)
    await waitFor(()=>expect(fetch).toHaveBeenCalledWith('/api/v2/jobs/79/retry',expect.objectContaining({method:'POST'})))
    expect(await screen.findByText(/attempt budget unchanged/i)).toBeInTheDocument()
    await userEvent.click(dismiss)
    await waitFor(()=>expect(fetch).toHaveBeenCalledWith('/api/v2/jobs/79/dismiss',expect.objectContaining({method:'POST'})))
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
      if(url.endsWith('/api/v2/series/merge')&&init?.method==='POST')return response({operation:{id:70,action:'accepted',status:'queued',representative_id:0,decision_ids:[],proposal_ids:[],series_ids:[7,9],job_id:170,error_code:'',error_message:'',created_at:'2026-08-05T12:00:00Z',updated_at:'2026-08-05T12:00:00Z',completed_at:null},created:true})
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
    expect(await screen.findByText(/Merge #70 is queued/)).toBeInTheDocument()
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
    const submitted=new Map<number,Record<string,unknown>>()
    const completed=new Set<number>()
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request,init?:RequestInit)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/providers'))return response({items:['asura','mangadex','mangafire','kingofshojo']})
      if(url.includes('/api/v2/matches/')&&init?.method==='POST'){
        const id=Number(url.split('/').at(-1));const value=JSON.parse(String(init.body)).decision
        const target=id===41?match:secondMatch
        const operation={id:100+id,action:value,status:'queued',representative_id:id,decision_ids:[id],proposal_ids:[id],series_ids:[target.left.id,target.right.id],job_id:200+id,error_code:'',error_message:'',created_at:'2026-08-05T12:00:00Z',updated_at:'2026-08-05T12:00:00Z',completed_at:null}
        submitted.set(id,operation)
        return response({operation,created:true})
      }
      if(url.includes('/api/v2/matches'))return response({items:[match,duplicateMatch,secondMatch].filter(item=>!completed.has(item.id)&&!(completed.has(41)&&item.id===43)),next_cursor:null,total:3})
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
    await screen.findByText(/Split queued for/)
    completed.add(41)
    const splitCompleted={...submitted.get(41),status:'succeeded',completed_at:'2026-08-05T12:00:02Z'}
    act(()=>{
      window.dispatchEvent(new CustomEvent('manga-job-event',{detail:{kind:'match_operation',type:'enqueued',state:'queued',operation:splitCompleted}}))
      window.dispatchEvent(new CustomEvent('manga-job-event',{detail:{kind:'match_operation',type:'leased',state:'leased',operation:splitCompleted}}))
      window.dispatchEvent(new CustomEvent('manga-job-event',{detail:{kind:'match_operation',type:'succeeded',state:'succeeded',operation:splitCompleted}}))
      window.dispatchEvent(new CustomEvent('manga-job-event',{detail:{kind:'match_operation',type:'succeeded',state:'succeeded',operation:splitCompleted}}))
    })
    await waitFor(()=>expect(document.querySelectorAll('.match-card')).toHaveLength(1))
    expect(screen.getAllByText('Titles kept separate')).toHaveLength(1)
    expect(screen.queryByText(/Split queued for/)).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('button',{name:'Merge'}))
    await userEvent.click(await screen.findByRole('button',{name:'Confirm merge'}))
    await screen.findByText(/Merge queued for/)
    completed.add(42)
    window.dispatchEvent(new CustomEvent('manga-job-event',{detail:{kind:'match_operation',type:'succeeded',state:'succeeded',operation:{...submitted.get(42),status:'succeeded',completed_at:'2026-08-05T12:00:03Z'}}}))
    await waitFor(()=>expect(document.querySelectorAll('.match-card')).toHaveLength(0))
    expect(screen.getByText('Merge completed')).toBeInTheDocument()
    const requestsAfter=(fetch as ReturnType<typeof vi.fn>).mock.calls.filter(([input,init])=>
      String(input).includes('/api/v2/matches?')&&(!init||!(init as RequestInit).method),
    ).length
    expect(requestsAfter).toBe(requestsBefore)
  })

  it('reconciles completed matches when the browser misses the SSE event',async()=>{
    const match={
      id:81,decision_ids:[81],confidence:.91,evidence:[],blocked_reasons:[],operation:null,
      left:{...series,source_title:series.title,source:'asura',url:'https://asura.test/painter',cover_evidence_used:true},
      right:{...matchingSeries,source_title:matchingSeries.title,source:'mangafire',url:'https://mangafire.test/painter',cover_evidence_used:true},
    }
    const operation={id:181,action:'rejected',status:'queued',representative_id:81,decision_ids:[81],proposal_ids:[81],series_ids:[series.id,matchingSeries.id],job_id:281,error_code:'',error_message:'',created_at:'2026-08-05T12:00:00Z',updated_at:'2026-08-05T12:00:00Z',completed_at:null}
    let completed=false
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request,init?:RequestInit)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/providers'))return response({items:['asura','mangafire']})
      if(url.includes('/api/v2/matches/')&&init?.method==='POST'){
        completed=true
        return response({operation,created:true})
      }
      if(url.includes('/api/v2/matches'))return response({items:completed?[]:[match],next_cursor:null,total:completed?0:1})
      return response({items:[],next_cursor:null})
    }))

    renderApp('/matches')
    await userEvent.click((await screen.findAllByRole('button',{name:'Keep separate'})).find(button=>!button.hasAttribute('disabled'))!)
    await screen.findByText('Splitting…')
    expect(EventSourceMock.instances.at(-1)).toBeDefined()
    await waitFor(()=>expect(screen.getByText('No matches need review')).toBeInTheDocument(),{timeout:4_000})
    const matchReads=(fetch as ReturnType<typeof vi.fn>).mock.calls.filter(([input,init])=>
      String(input).includes('/api/v2/matches?')&&(!init||!(init as RequestInit).method),
    )
    expect(matchReads.length).toBeGreaterThan(1)
    expect((matchReads.at(-1)?.[1] as RequestInit).cache).toBe('no-store')
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
    const batchOperations=[
      {id:151,action:'accepted' as const,status:'queued' as const,representative_id:51,decision_ids:[51],proposal_ids:[51],series_ids:[series.id,matchingSeries.id],job_id:251,error_code:'',error_message:'',created_at:'2026-08-05T12:00:00Z',updated_at:'2026-08-05T12:00:00Z',completed_at:null},
      {id:153,action:'accepted' as const,status:'queued' as const,representative_id:53,decision_ids:[53],proposal_ids:[53],series_ids:[21,22],job_id:253,error_code:'',error_message:'',created_at:'2026-08-05T12:00:00Z',updated_at:'2026-08-05T12:00:00Z',completed_at:null},
    ]
    let previewBody:{ids:number[];entire_queue:boolean}|null=null
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request,init?:RequestInit)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/providers'))return response({items:['asura','mangadex','mangafire','kingofshojo']})
      if(url.endsWith('/api/v2/match-batch/preview')){
        previewBody=JSON.parse(String(init?.body))
        return response({selected:previewBody!.ids.length,eligible:previewBody!.ids.length,blocked:0,items:[]})
      }
      if(url.endsWith('/api/v2/match-batch')&&init?.method==='POST')return response({operations:batchOperations,ids:[51,53],blocked:[]})
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
    expect(previewBody).toEqual({ids:[51,53],excluded_ids:[],entire_queue:false,decision:'rejected'})
    await userEvent.click(screen.getByRole('button',{name:'Confirm merge'}))
    expect(await screen.findByText('Merge batch · 0 of 2 complete')).toBeInTheDocument()
    let progress=screen.getByRole('progressbar',{name:'Batch operation progress'})
    expect(progress).toHaveAttribute('aria-valuenow','0')
    act(()=>window.dispatchEvent(new CustomEvent('manga-job-event',{detail:{kind:'match_operation',type:'succeeded',state:'succeeded',operation:{...batchOperations[0],status:'succeeded',completed_at:'2026-08-05T12:00:02Z'}}})))
    expect(await screen.findByText('Merge batch · 1 of 2 complete')).toBeInTheDocument()
    expect(document.querySelectorAll('.toast')).toHaveLength(1)
    act(()=>window.dispatchEvent(new CustomEvent('manga-job-event',{detail:{kind:'match_operation',type:'succeeded',state:'succeeded',operation:{...batchOperations[1],status:'succeeded',completed_at:'2026-08-05T12:00:03Z'}}})))
    expect(await screen.findByText('Merge batch completed · 2 of 2')).toBeInTheDocument()
    progress=screen.getByRole('progressbar',{name:'Batch operation progress'})
    expect(progress).toHaveAttribute('aria-valuenow','2')
    expect(document.querySelectorAll('.toast')).toHaveLength(1)
  })

  it('keeps failed async match cards actionable and shows a dismissible locator',async()=>{
    const match={
      id:71,decision_ids:[71],confidence:.91,evidence:[],blocked_reasons:[],operation:null,
      left:{...series,source_title:series.title,source:'asura',url:'https://asura.test/painter',cover_evidence_used:true},
      right:{...matchingSeries,source_title:matchingSeries.title,source:'mangafire',url:'https://mangafire.test/painter',cover_evidence_used:true},
    }
    const operation={id:171,action:'rejected',status:'queued',representative_id:71,decision_ids:[71],proposal_ids:[71],series_ids:[series.id,matchingSeries.id],job_id:271,error_code:'',error_message:'',created_at:'2026-08-05T12:00:00Z',updated_at:'2026-08-05T12:00:00Z',completed_at:null}
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request,init?:RequestInit)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/providers'))return response({items:['asura','mangafire']})
      if(url.includes('/api/v2/matches/')&&init?.method==='POST')return response({operation,created:true})
      if(url.includes('/api/v2/matches'))return response({items:[match],next_cursor:null,total:1})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/matches')
    await userEvent.click((await screen.findAllByRole('button',{name:'Keep separate'})).find(button=>!button.hasAttribute('disabled'))!)
    await screen.findByText('Splitting…')
    window.dispatchEvent(new CustomEvent('manga-job-event',{detail:{kind:'match_operation',operation:{...operation,status:'failed',error_code:'match_operation_failed',error_message:'Database conflict',completed_at:'2026-08-05T12:00:02Z'}}}))
    expect(await screen.findByText('Split failed')).toBeInTheDocument()
    expect(screen.queryByText(/Split queued for/)).not.toBeInTheDocument()
    expect(document.querySelector('.match-card')).toHaveClass('operation-failed')
    expect(screen.getByRole('button',{name:'Merge'})).not.toBeDisabled()
    await userEvent.click(screen.getByRole('button',{name:'Dismiss notification'}))
    expect(screen.queryByText('Split failed')).not.toBeInTheDocument()
  })

  it('keeps queue-wide selection editable and reflects exclusions in the master checkbox',async()=>{
    const match={
      id:61,decision_ids:[61],confidence:.91,evidence:[],blocked_reasons:[],
      left:{...series,source_title:series.title,source:'asura',url:'https://asura.test/painter',cover_evidence_used:true},
      right:{...matchingSeries,source_title:matchingSeries.title,source:'mangafire',url:'https://mangafire.test/painter',cover_evidence_used:true},
    }
    const other={...match,id:62,decision_ids:[62],confidence:.72,left:{...match.left,id:31,title:'Low Left'},right:{...match.right,id:32,title:'Low Right'}}
    let previewBody:{ids:number[];excluded_ids:number[];entire_queue:boolean}|null=null
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request,init?:RequestInit)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/providers'))return response({items:['asura','mangadex','mangafire','kingofshojo']})
      if(url.endsWith('/api/v2/match-batch/preview')){previewBody=JSON.parse(String(init?.body));return response({selected:1,eligible:1,blocked:0,items:[]})}
      if(url.includes('/api/v2/matches'))return response({items:url.includes('order=asc')?[other,match]:[match,other],next_cursor:null})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/matches')
    await waitFor(()=>expect(document.querySelectorAll('.match-card')).toHaveLength(2))
    const master=screen.getByRole('checkbox',{name:/Select entire queue/i}) as HTMLInputElement
    await userEvent.click(master)
    expect(master).toBeChecked()
    let pairCheckboxes=screen.getAllByRole('checkbox').slice(1) as HTMLInputElement[]
    expect(pairCheckboxes.every(box=>box.checked&&!box.disabled)).toBe(true)
    await userEvent.click(pairCheckboxes[0])
    expect(master).not.toBeChecked()
    expect(pairCheckboxes[0]).not.toBeChecked()
    await userEvent.click(screen.getByRole('button',{name:'Preview'}))
    await waitFor(()=>expect(previewBody).toEqual({ids:[],excluded_ids:[61],entire_queue:true,decision:'rejected'}))
    pairCheckboxes=screen.getAllByRole('checkbox').slice(1) as HTMLInputElement[]
    await userEvent.click(pairCheckboxes[0])
    expect(master).toBeChecked()
    await userEvent.click(screen.getByRole('button',{name:'Reverse merge result order'}))
    await waitFor(()=>expect(fetch).toHaveBeenCalledWith(expect.stringContaining('order=asc'),expect.anything()))
    expect(screen.getByRole('button',{name:'Reverse merge result order'})).toHaveTextContent('Lowest first')
  })

  it('prefetches the next match page well before the current page ends',async()=>{
    const first={
      id:81,decision_ids:[81],confidence:.91,evidence:[],blocked_reasons:[],operation:null,
      left:{...series,source_title:series.title,source:'asura',url:'https://asura.test/painter',cover_evidence_used:true},
      right:{...matchingSeries,source_title:matchingSeries.title,source:'mangafire',url:'https://mangafire.test/painter',cover_evidence_used:true},
    }
    const second={...first,id:82,decision_ids:[82],left:{...first.left,id:41,title:'Prefetched Left',source_title:'Prefetched Left'},right:{...first.right,id:42,title:'Prefetched Right',source_title:'Prefetched Right'}}
    let observerCallback:IntersectionObserverCallback|undefined
    let observerOptions:IntersectionObserverInit|undefined
    vi.stubGlobal('IntersectionObserver',class{
      constructor(callback:IntersectionObserverCallback,options?:IntersectionObserverInit){observerCallback=callback;observerOptions=options}
      observe=vi.fn();disconnect=vi.fn();unobserve=vi.fn();takeRecords=()=>[]
    })
    vi.stubGlobal('fetch',vi.fn((input:string|URL|Request)=>{
      const url=String(input)
      if(url.includes('/api/v2/operations'))return response({job_counts:{},health:{series:2,chapters:1,active_artifacts:0,missing_projections:0},sources:[],workers:[],permits:{}})
      if(url.includes('/api/v2/providers'))return response({items:['asura','mangafire']})
      if(url.includes('/api/v2/matches'))return url.includes('cursor=82')
        ? response({items:[second],next_cursor:null})
        : response({items:[first],next_cursor:82})
      return response({items:[],next_cursor:null})
    }))
    renderApp('/matches')
    await waitFor(()=>expect(observerCallback).toBeDefined())
    expect(observerOptions?.rootMargin).toBe('2400px 0px')
    act(()=>observerCallback?.([{isIntersecting:true} as IntersectionObserverEntry],{} as IntersectionObserver))
    await waitFor(()=>expect(fetch).toHaveBeenCalledWith(expect.stringContaining('cursor=82'),expect.anything()))
    expect(await screen.findByText('Prefetched Left')).toBeInTheDocument()
  })
})
