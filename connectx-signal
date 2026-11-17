import {DurableObject} from 'cloudflare:workers';
export default {fetch(req,env){const u=new URL(req.url);
 if(u.pathname==='/ws'&&req.headers.get('Upgrade')==='websocket')return env.HUB.get(env.HUB.idFromName('hub')).fetch(req);
 return new Response('ConnectX signalling OK')}};
export class Hub extends DurableObject{
 async fetch(req){const u=new URL(req.url),n=u.searchParams.get('n')||'',k=u.searchParams.get('k')||'';
  const [c,s]=Object.values(new WebSocketPair());this.ctx.acceptWebSocket(s,[n]);
  if(!/^\d{6,15}$/.test(n)||k.length<16||k.length>64){s.close(4002,'bad');return new Response(null,{status:101,webSocket:c})}
  const old=await this.ctx.storage.get('k:'+n);
  if(!old)await this.ctx.storage.put('k:'+n,k);
  else if(old!==k){s.close(4001,'taken');return new Response(null,{status:101,webSocket:c})}
  for(const w of this.ctx.getWebSockets(n))if(w!==s)try{w.close(4000,'replaced')}catch{}
  const up=JSON.stringify({t:'up',n});
  for(const w of this.ctx.getWebSockets())if(w!==s)try{w.send(up)}catch{}
  return new Response(null,{status:101,webSocket:c})}
 webSocketMessage(ws,msg){if(typeof msg!=='string'||msg.length>65536)return;let m;try{m=JSON.parse(msg)}catch{return}
  if(m.t!=='sig')return;m.from=this.ctx.getTags(ws)[0];
  const t=this.ctx.getWebSockets(String(m.to));
  if(t.length)t[0].send(JSON.stringify(m));else ws.send(JSON.stringify({t:'off',n:String(m.to)}))}
 webSocketClose(ws,code){try{ws.close(code)}catch{}}}
