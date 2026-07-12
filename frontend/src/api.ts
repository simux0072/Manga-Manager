import type {ActivityEvent, Job, Match, Operations, Page, Series, UpdateSeries} from './types'

async function request<T>(url:string, init?:RequestInit):Promise<T>{
  const response=await fetch(url,{...init,headers:{'Content-Type':'application/json',...(init?.headers||{})}})
  if(!response.ok){const body=await response.json().catch(()=>({detail:response.statusText}));throw new Error(body.detail||'Request failed')}
  return response.json()
}
export function params(values:Record<string,string|string[]|number|undefined>){const p=new URLSearchParams();Object.entries(values).forEach(([key,value])=>{if(Array.isArray(value))value.forEach(item=>p.append(key,item));else if(value!==undefined&&value!=='')p.set(key,String(value))});return p.toString()}
export const api={
  discovery:(q:string,sources:string[],cursor?:string,signal?:AbortSignal)=>request<Page<Series>>(`/api/v2/discovery?${params({q,source:sources,cursor})}`,{signal}),
  library:(q:string,sources:string[],states:string[],cursor?:string)=>request<Page<Series>>(`/api/v2/library?${params({q,source:sources,state:states,cursor})}`),
  updates:(cursor?:string)=>request<Page<UpdateSeries>>(`/api/v2/updates?${params({cursor})}`),
  changeSeries:(id:number,status:string)=>request<{item:Series;previous:string}>(`/api/v2/series/${id}`,{method:'PATCH',body:JSON.stringify({status})}),
  changeChapter:(id:number,status:string)=>request(`/api/v2/chapters/${id}`,{method:'PATCH',body:JSON.stringify({status})}),
  readAll:(id:number)=>request(`/api/v2/series/${id}/chapters/read`,{method:'POST'}),
  matches:(cursor?:number)=>request<Page<Match,number>>(`/api/v2/matches?${params({cursor})}`),
  decideMatch:(id:number,decision:string,confirmation='')=>request(`/api/v2/matches/${id}`,{method:'POST',body:JSON.stringify({decision,confirmation})}),
  decideMatches:(ids:number[],decision:string,confirmation='')=>request('/api/v2/match-batch',{method:'POST',body:JSON.stringify({ids,decision,confirmation})}),
  jobs:(states:string[]=[],cursor?:number,limit=30)=>request<Page<Job,number>>(`/api/v2/jobs?${params({state:states,cursor,limit})}`),
  activity:(types:string[]=[],sources:string[]=[],cursor?:number)=>request<Page<ActivityEvent,number>>(`/api/v2/activity?${params({event_type:types,source:sources,cursor})}`),
  operations:()=>request<Operations>('/api/v2/operations'),
  retryJob:(id:number)=>request(`/api/v2/jobs/${id}/retry`,{method:'POST'}),
  pullSource:(source:string)=>request(`/api/v2/sources/${source}/pull`,{method:'POST'}),
  probe:()=>request('/api/v2/probe',{method:'POST'})
}
