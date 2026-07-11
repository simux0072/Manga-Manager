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
    expect(screen.getByRole('button',{name:'failed'})).toBeInTheDocument()
    expect(screen.getByRole('button',{name:'running'})).toBeInTheDocument()
  })
})
