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
})
