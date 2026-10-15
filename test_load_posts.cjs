const {test} = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const source = fs.readFileSync(process.env.LOAD_POSTS_TEMPLATE || 'templates/index.html','utf8').split('<script>')[1].split('</script>')[0];
function setup(fetch) {
  const listeners = {};
  const elements = {};
  function element() { return {innerHTML:'', textContent:'', value:'', disabled:false, children:[], classList:{remove(){}}, appendChild(el){this.children.push(el);}}; }
  for (const id of ['posts','posts-area','load-posts','post-limit','since']) elements[id] = element();
  elements['post-limit'].value='2'; elements.since.value='2026-01-01';
  const pages = [{value:'p',dataset:{name:'Good'}},{value:'q',dataset:{name:'<Bad>'}}];
  const ctx = vm.createContext({console, fetch, AbortController, URLSearchParams, alert(){}, window:{addEventListener(){}}, document:{getElementById:id=>elements[id], querySelectorAll:()=>pages, createElement:element, addEventListener:(name,fn)=>{(listeners[name]??=[]).push(fn);}}});
  vm.runInContext(source,ctx); vm.runInContext("sessionId='s'",ctx);
  return {ctx,elements,pages,listeners,load:()=>vm.runInContext('loadSelectedPages()',ctx)};
}
const response = (data, status=200) => ({ok:status===200,status,json:async()=>data});
const post = {id:'p_1',message:'visible',created_time:'2026-01-02',permalink_url:'https://example.com'};
test('one page failure preserves posts and filter, never renders false empty',async()=>{
  let calls=[];
  const s=setup(async url=>{calls.push(url);return calls.length===1 ? response({posts:[post]}) : response({detail:'secret'},500);});
  await s.load();
  assert.match(s.elements.posts.innerHTML,/visible/);
  assert.doesNotMatch(s.elements.posts.innerHTML,/Tidak ada postingan/);
  assert.ok(s.elements.posts.children.some(e=>e.textContent.includes('Gagal memuat')));
  assert.ok(s.elements.posts.children.every(e=>!e.textContent.includes('secret')));
  assert.ok(s.elements.posts.children.some(e=>e.textContent.includes('2026-01-01')));
  assert.match(calls[0],/limit=2&since=2026-01-01/);
  assert.equal(s.elements['load-posts'].disabled,false);
});
test('double click and stale session response cannot overwrite new load',async()=>{
  let resolve, count=0;
  const s=setup(async()=>{count++; if(count===1) return new Promise(r=>resolve=r); return response({posts:[{...post,message:'new result'}]});});
  s.pages.splice(1);
  const old=s.load(); await s.load(); assert.equal(count,1);
  vm.runInContext("sessionId='new'",s.ctx);
  await s.load(); resolve(response({posts:[{...post,message:'old result'}]})); await old;
  assert.match(s.elements.posts.innerHTML,/new result/);
  assert.doesNotMatch(s.elements.posts.innerHTML,/old result/);
});
test('logout intent cancels pending results and releases button',async()=>{
  let resolve;
  const s=setup(()=>new Promise(r=>resolve=r));
  const pending=s.load();
  s.listeners.click[0]({target:{closest:()=>true}});
  s.elements.posts.innerHTML='cleared';
  resolve(response({posts:[post]})); await pending;
  assert.equal(s.elements.posts.innerHTML,'cleared');
  assert.equal(s.elements['load-posts'].disabled,false);
});
for (const [name,fetch] of [
  ['invalid JSON',async()=>({ok:true,status:200,json:async()=>{throw new SyntaxError();}})],
  ['network',async()=>{throw new TypeError('secret');}],
  ['invalid structure',async()=>response({})],
  ['API error',async()=>response({detail:'secret'},400)]
]) test(name+' is failure, not empty',async()=>{
  const s=setup(fetch); await s.load();
  assert.doesNotMatch(s.elements.posts.innerHTML,/Tidak ada postingan/);
  assert.ok(s.elements.posts.children.some(e=>e.textContent.includes('Gagal memuat')));
  assert.equal(s.elements['load-posts'].disabled,false);
});
test('valid empty is displayed',async()=>{
  const s=setup(async()=>response({posts:[]})); await s.load();
  assert.match(s.elements.posts.innerHTML,/Tidak ada postingan/);
  assert.ok(!s.elements.posts.children.some(e=>e.textContent.includes('Gagal memuat')));
});
test('inputs are snapshotted and partial warning preserves checkbox/thumbnail contract',async()=>{
  let resolve, calls=[];
  const s=setup(async url=>{calls.push(url); if(calls.length===1)return new Promise(r=>resolve=r);return response({posts:[post]});});
  const pending=s.load();
  s.pages[1].value='changed'; s.pages[1].dataset.name='changed';
  s.elements['post-limit'].value='100'; s.elements.since.value='2020-01-01';
  resolve(response({posts:[{...post,full_picture:'https://example.com/thumb'}],partial:true,warning:'Batas request pemuatan tercapai'}));
  await pending;
  assert.match(calls[1],/\/q\?limit=2&since=2026-01-01/);
  assert.match(s.elements.posts.innerHTML,/class="post-check/);
  assert.match(s.elements.posts.innerHTML,/data-page="p"/);
  assert.match(s.elements.posts.innerHTML,/https:\/\/example.com\/thumb/);
  assert.ok(s.elements.posts.children.some(e=>e.textContent.includes('Hasil parsial')));
});
