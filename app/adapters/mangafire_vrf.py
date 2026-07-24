from __future__ import annotations

import asyncio
import json
import re

import quickjs

from app.adapters.http import HttpSourceClient


_CONFIG_RE = re.compile(r'window\.__config\s*=\s*"([^"]+)"')
_POLYFILL_RE = re.compile(r'href="(https:[^"]*polyfill[^"]+\.js)"')
_EXPORT_RE = re.compile(r"export\{[^}]+\};?\s*$")
_ANTI_DEBUG_RE = re.compile(
    r",globalThis\.DisableDevtool=t\.DisableDevtool,.*?,n\(20,"
)
_BROWSER_HEADERS = {
    "Referer": "https://mangafire.to/",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) "
        "Gecko/20100101 Firefox/140.0"
    ),
}

_BROWSER_SHIMS = r"""
var window=globalThis;
var self=globalThis;
function setInterval(){return 0}
function clearInterval(){}
function setTimeout(){return 0}
function clearTimeout(){}
const B64="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
function atob(input){
  let output="",bc=0,bs,buffer,index=0;
  input=String(input).replace(/=+$/,"");
  while((buffer=input.charAt(index++))){
    buffer=B64.indexOf(buffer);
    if(buffer<0)continue;
    bs=bc%4?bs*64+buffer:buffer;
    if(bc++%4)output+=String.fromCharCode(255&bs>>(-2*bc&6));
  }
  return output;
}
function btoa(input){
  let output="",c1,c2,c3,e1,e2,e3,e4,index=0;
  while(index<input.length){
    c1=input.charCodeAt(index++);
    c2=input.charCodeAt(index++);
    c3=input.charCodeAt(index++);
    e1=c1>>2;
    e2=(c1&3)<<4|c2>>4;
    e3=(c2&15)<<2|c3>>6;
    e4=c3&63;
    if(isNaN(c2))e3=e4=64;
    else if(isNaN(c3))e4=64;
    output+=B64[e1]+B64[e2]+B64[e3]+B64[e4];
  }
  return output;
}
class TextEncoder{
  encode(value){
    value=unescape(encodeURIComponent(value));
    let output=new Uint8Array(value.length);
    for(let index=0;index<value.length;index++)output[index]=value.charCodeAt(index);
    return output;
  }
}
var location={hostname:"mangafire.to",href:"https://mangafire.to/"};
var navigator={appCodeName:"Mozilla",userAgent:"Mozilla/5.0",platform:"Linux"};
"""


class MangaFireVrf:
    """Evaluate MangaFire's rotating public request-token routine in a bounded JS isolate."""

    def __init__(self, client: HttpSourceClient) -> None:
        self.client = client
        self._context: quickjs.Context | None = None
        self._lock = asyncio.Lock()

    async def token(self, path: str, params: dict[str, object] | None = None) -> str:
        async with self._lock:
            if self._context is None:
                await self._refresh()
            return self._evaluate(path, params or {})

    async def refresh(self) -> None:
        async with self._lock:
            await self._refresh()

    def _evaluate(self, path: str, params: dict[str, object]) -> str:
        if self._context is None:
            raise RuntimeError("MangaFire token context is not initialized")
        expression = (
            f"getProtectionToken({json.dumps(path)},"
            f"JSON.parse({json.dumps(json.dumps(params, separators=(',', ':')))}))"
        )
        token = self._context.eval(expression)
        if not isinstance(token, str) or not token:
            raise RuntimeError("MangaFire returned an empty API token")
        return token

    async def _refresh(self) -> None:
        homepage = await self.client.request(
            "GET",
            f"{self.client.base_url}/",
            headers=_BROWSER_HEADERS,
        )
        config_match = _CONFIG_RE.search(homepage.text)
        script_match = _POLYFILL_RE.search(homepage.text)
        if config_match is None or script_match is None:
            raise RuntimeError("MangaFire token configuration was not found")
        script = (
            await self.client.request(
                "GET",
                script_match.group(1),
                headers=_BROWSER_HEADERS,
            )
        ).text
        script = _EXPORT_RE.sub("", script)
        script, substitutions = _ANTI_DEBUG_RE.subn(
            ",globalThis.DisableDevtool=t.DisableDevtool,n(20,",
            script,
            count=1,
        )
        if substitutions != 1:
            raise RuntimeError("MangaFire token module layout changed")

        context = quickjs.Context()
        context.set_memory_limit(64 * 1024 * 1024)
        context.set_max_stack_size(2 * 1024 * 1024)
        context.set_time_limit(5)
        context.eval(
            _BROWSER_SHIMS
            + f"\nwindow.__config={json.dumps(config_match.group(1))};"
        )
        context.eval(script)
        self._context = context
