"use strict";
const $ = id => document.getElementById(id);
let token = location.hash.slice(1) || sessionStorage.getItem("pybox-token") || "";
if (location.hash) { sessionStorage.setItem("pybox-token", token); history.replaceState(null, "", "/"); }
let overview = null, selected = null, detail = null, tab = "stdout", currentView = "overview", busy = false, settingsLoaded = false;
const labels = {queued:"대기 중",verifying:"검증 중",preparing:"준비 중",starting:"시작 중",running:"실행 중",stopping:"중지 중",completed:"완료",failed:"실패",cancelled:"취소됨",interrupted:"중단 · 확인 필요"};
const terminal = state => ["completed","failed","cancelled","interrupted"].includes(state);
const time = value => value ? new Date(value).toLocaleTimeString("ko-KR",{hour12:false}) : "—";
const duration = job => job.started_at ? (((job.finished_at || Date.now())-job.started_at)/1000).toFixed(1)+"s" : "—";
function node(tag, text, cls) { const n=document.createElement(tag); if(text!==undefined)n.textContent=text; if(cls)n.className=cls; return n; }
function toast(text) { $("toast").textContent=text; $("toast").classList.remove("hidden"); clearTimeout(toast.timer); toast.timer=setTimeout(()=>$("toast").classList.add("hidden"),4500); }
async function api(path, body) {
  const controller = new AbortController(), timer = setTimeout(()=>controller.abort(),8000);
  try {
    const response = await fetch(path,{method:body===undefined?"GET":"POST",headers:{Authorization:`Bearer ${token}`,"Content-Type":"application/json"},body:body===undefined?undefined:JSON.stringify(body),signal:controller.signal});
    const result=await response.json();
    if(!response.ok)throw new Error(result.error||`HTTP ${response.status}`);
    return result;
  } finally {clearTimeout(timer);}
}
function badge(state) { return node("span",labels[state]||state,`badge ${state}`); }
function table(target, jobs) {
  const fragment=document.createDocumentFragment();
  for(const job of jobs) {
    const row=node("tr");
    const title=node("td"), button=node("button",job.project,"job-title"); button.addEventListener("click",()=>openDetail(job.id));
    title.append(button,node("code",job.id,"job-id")); row.append(title);
    const state=node("td");state.append(badge(job.state));row.append(state);
    row.append(node("td",job.action==="verify"?"무결성 검사":job.action==="prepare"?"스냅샷 준비":job.config.cold?"Cold boot":"스냅샷 복원"));
    row.append(node("td",time(job.started_at||job.created_at)),node("td",duration(job)));
    row.append(node("td",job.cleanup_verified===true?"확인됨":job.cleanup_verified===false?"미확인":"—"));
    const action=node("td"), control=node("button",terminal(job.state)?"상세 ↗":"중지","row-action");
    control.addEventListener("click",()=>terminal(job.state)?openDetail(job.id):stop(job.id));action.append(control);row.append(action);fragment.append(row);
  }
  // Avoid replacing a focused row button during refresh.
  if(!$(target).contains(document.activeElement))$(target).replaceChildren(fragment);
}
function pairs(target, values) { const f=document.createDocumentFragment(); for(const [key,value] of values)f.append(node("dt",key),node("dd",String(value)));$(target).replaceChildren(f); }
function render() {
  if(!overview)return;
  const {counts,settings,image}=overview;
  $("version").textContent=`Pybox v${overview.version}`;
  $("live-count").textContent=counts.live_vms;$("slot-label").textContent=`/ ${settings.max_concurrent} 작업 한도`;
  $("active-count").textContent=counts.active;$("queued-count").textContent=counts.queued;$("done-count").textContent=counts.completed;$("failed-count").textContent=`/ ${counts.failed}`;$("history-count").textContent=`전체 ${counts.total}개 · 취소 ${counts.cancelled}개`;
  $("capability").textContent=overview.capability.available?`${overview.capability.mechanism} 사용 가능`:`${overview.capability.mechanism} 사용 불가`;
  $("cap-dot").classList.toggle("bad",!overview.capability.available);
  const warning=overview.unhealthy || (overview.bundle_cleanup_blocked?"번들에 자원 회수 미확인 기록이 있습니다. 새 실행이 차단됩니다.":null);
  $("warning").classList.toggle("hidden",!warning);$("warning").textContent=warning?`확인 필요 · ${warning}`:"";
  document.querySelectorAll(".launch-button").forEach(b=>b.disabled=!!warning||!overview.capability.available);
  $("stop-all").disabled=counts.active+counts.queued===0;
  const filter=$("filter").value;
  const filtered=overview.jobs.filter(j=>filter==="all"||(filter==="active"&&!terminal(j.state)&&j.state!=="queued")||(filter==="failed"&&["failed","interrupted"].includes(j.state))||j.state===filter);
  table("jobs",filtered);$("empty").classList.toggle("hidden",filtered.length>0);$("list-count").textContent=`${filtered.length}개 작업`;
  if(overview.jobs.length && !filtered.length){$("empty").querySelector("h3").textContent="해당 상태의 작업이 없습니다";$("empty").querySelector("p").textContent="필터를 바꾸어 다른 작업을 확인하세요.";}
  else {$("empty").querySelector("h3").textContent="아직 실행한 샌드박스가 없습니다";$("empty").querySelector("p").textContent="프로젝트 이름을 지정하고 첫 번째 작업을 시작하세요. 실제 VM에서 Python 검증 워크로드를 실행합니다.";}
  const history=overview.jobs.filter(j=>terminal(j.state));table("history-jobs",history);$("history-empty").classList.toggle("hidden",history.length>0);
  const m=image.manifest||{};
  pairs("image-details",[["번들 무결성",image.integrity==="verified"?"검증됨":"아직 검사하지 않음"],["마지막 번들 검사",image.last_verified_at?new Date(image.last_verified_at).toLocaleString("ko-KR"):"—"],["이미지 크기",m.rootfs_bytes?`${(m.rootfs_bytes/1048576).toFixed(1)} MiB`:"—"],["이미지 ID",m.image_id||"—"],["Rootfs SHA-256",m.rootfs_sha256||"—"],["번들 위치",image.bundle_path]]);
  pairs("snapshot-details",[["파일 존재",image.snapshot_present?"준비 파일 있음":"준비 전"],["검증 방식","준비·복원 시 전체 목록과 해시 확인"],["게스트 메모리","512 MiB"],["준비 제한 시간","45초"],["상태 보존","각 실행의 VM 종료 시 폐기"]]);
  if(!settingsLoaded){$("setting-concurrency").value=settings.max_concurrent;$("setting-timeout").value=settings.timeout_ms;$("setting-mode").value=settings.cold?"cold":"warm";settingsLoaded=true;}
}
async function refresh() {
  if(busy)return;busy=true;
  try {
    overview=await api("/api/overview");render();
    $("connection").replaceChildren(node("span",undefined,"status-dot"),node("span","연결됨 · 실시간 갱신"));$("auth").classList.add("hidden");
    if(selected&&$("detail-dialog").open){detail=await api(`/api/jobs/${selected}`);renderDetail();}
  } catch(error) {$("connection").replaceChildren(node("span",undefined,"status-dot bad"),node("span","연결 끊김 · 값은 마지막 관측"));if(!overview)$("auth").classList.remove("hidden");}
  finally {busy=false;}
}
function view(name){currentView=name;document.querySelectorAll(".view").forEach(v=>v.classList.toggle("hidden",v.id!==name+"-view"));document.querySelectorAll(".nav").forEach(v=>v.classList.toggle("active",v.dataset.view===name));$("page-name").textContent={overview:"전체 현황",history:"실행 이력",image:"이미지 · 스냅샷",settings:"실행 설정"}[name];}
document.querySelectorAll(".nav").forEach(b=>b.addEventListener("click",()=>view(b.dataset.view)));
document.querySelectorAll("[data-goto]").forEach(b=>b.addEventListener("click",()=>view(b.dataset.goto)));
$("filter").addEventListener("change",render);
document.querySelectorAll(".launch-button").forEach(button=>button.addEventListener("click",()=>{if(!overview)return;$("run-timeout").value=overview.settings.timeout_ms;$("run-mode").value=overview.settings.cold?"cold":"warm";$("launch-error").textContent="";$("launch-dialog").showModal();$("project").focus();}));
for(const id of ["close-launch","cancel-launch"])$(id).addEventListener("click",()=>$("launch-dialog").close());
$("launch-form").addEventListener("submit",async event=>{event.preventDefault();const button=event.submitter;button.disabled=true;try{const job=await api("/api/jobs",{project:$("project").value,cold:$("run-mode").value==="cold",timeout_ms:Number($("run-timeout").value)});$("launch-dialog").close();toast("실행 요청을 접수했습니다.");await refresh();await openDetail(job.id);}catch(error){$("launch-error").textContent=error.message;}finally{button.disabled=false;}});
async function stop(id){try{await api(`/api/jobs/${id}/stop`,{});toast("중지를 요청했습니다. 자원 회수 결과를 확인 중입니다.");await refresh();}catch(error){toast(error.message);}}
$("stop-all").addEventListener("click",async()=>{if(!confirm("진행 중이거나 대기 중인 모든 작업을 중지할까요?"))return;try{await api("/api/stop-all",{});toast("전체 중지를 요청했습니다.");await refresh();}catch(error){toast(error.message);}});
$("settings-form").addEventListener("submit",async event=>{event.preventDefault();event.submitter.disabled=true;try{await api("/api/settings",{max_concurrent:Number($("setting-concurrency").value),timeout_ms:Number($("setting-timeout").value),cold:$("setting-mode").value==="cold"});toast("기본값을 저장했습니다. 접수된 작업의 설정은 유지됩니다.");await refresh();}catch(error){toast(error.message);}finally{event.submitter.disabled=false;}});
async function imageAction(action){try{const job=await api("/api/jobs",{project:"Runtime maintenance",action});toast("이미지 작업을 접수했습니다.");await refresh();await openDetail(job.id);}catch(error){toast(error.message);}}
$("verify-image").addEventListener("click",()=>imageAction("verify"));$("prepare-image").addEventListener("click",()=>imageAction("prepare"));
async function openDetail(id){try{detail=await api(`/api/jobs/${id}`);selected=id;renderDetail();if(!$("detail-dialog").open)$("detail-dialog").showModal();}catch(error){toast(error.message);}}
$("close-detail").addEventListener("click",()=>{$("detail-dialog").close();selected=null;});$("detail-dialog").addEventListener("close",()=>selected=null);
$("stop-job").addEventListener("click",()=>selected&&stop(selected));
document.querySelectorAll(".tab").forEach(button=>button.addEventListener("click",()=>{tab=button.dataset.tab;document.querySelectorAll(".tab").forEach(b=>b.classList.toggle("active",b===button));renderDetail();if(tab==="result")$("detail-log").scrollTop=0;}));
function renderDetail(){
  if(!detail)return;const j=detail;
  $("detail-project").textContent=j.project;$("detail-id").textContent=j.id;$("detail-state").replaceChildren(badge(j.state));$("stop-job").disabled=terminal(j.state)||j.cancel_requested;
  pairs("detail-config",[["실행 대상",j.action==="run"?"고정 Python 에이전트 검증":j.action==="prepare"?"스냅샷 준비":"무결성 검사"],["현재 / 마지막 PID",`${j.pid??"—"} / ${j.last_pid??"—"}`],["요청 / 경과",`${new Date(j.created_at).toLocaleString("ko-KR")} · ${duration(j)}`],["적용된 실행 방식",j.config.cold?"Cold boot":"스냅샷 복원"],["적용된 제한 시간",`${j.config.timeout_ms.toLocaleString()} ms`],["메모리 / 출력","512 MiB / stdout·stderr 각 8 KiB"],["호스트 실행 정책",j.config.host_execution_policy],["자원 회수",j.cleanup_verified===true?"확인됨":j.cleanup_verified===false?"미확인 · 새 실행 차단":"진행 중 또는 아직 실행 전"],["이미지 해시",j.config.rootfs_sha256]]);
  $("detail-error").textContent=j.error||"";
  const log=$("detail-log"),nearBottom=log.scrollHeight-log.scrollTop-log.clientHeight<30;
  const text=tab==="events"?j.events.map(e=>JSON.stringify(e)).join("\n"):tab==="result"?JSON.stringify(j.result,null,2):j[tab];
  const next=text&&text!=="null"?text:"아직 기록이 없습니다.";
  if(log.textContent!==next){log.textContent=next;if(nearBottom)log.scrollTop=log.scrollHeight;}
  $("detail-note").textContent=j.logs_truncated?"관리 로그 표시 한도에 도달했습니다. 일부 출력이 생략되었습니다.":"준비와 실행의 관측 출력입니다. 자원 회수 완료는 실제 프로세스 종료·출력 회수 결과로 판정합니다.";
}
refresh();setInterval(()=>{if(!document.hidden)refresh();},1000);document.addEventListener("visibilitychange",()=>{if(!document.hidden)refresh();});
