import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { eq } from "drizzle-orm";
import { agents, companies, createDb, environmentLeases, environments, heartbeatRuns, issues, plugins } from "@paperclipai/db";
import { environmentRuntimeService } from "../services/environment-runtime.js";
import { resolvePluginTaskResume } from "../services/environment-task-runtime.js";
import type { PluginWorkerManager } from "../services/plugin-worker-manager.js";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";

const support=await getEmbeddedPostgresTestSupport();
(support.supported ? describe : describe.skip)("retained Codex plugin task leases",()=>{
 let db:ReturnType<typeof createDb>,stop:()=>Promise<void>;
 beforeAll(async()=>{const started=await startEmbeddedPostgresTestDatabase("plugin-warm-tasks");db=createDb(started.connectionString);stop=started.stop;});
 afterAll(async()=>{await stop?.();});
 async function fixture(){
  const companyId=randomUUID(),agentId=randomUUID(),issueId=randomUUID(),environmentId=randomUUID(),pluginId=randomUUID();
  let runId=randomUUID(),allocation=0,stopped=false,outage=false;
  const calls:string[]=[];
  let resumeGate:Promise<void>|undefined,releaseResume:(()=>void)|undefined,resumes=0;
  await db.insert(companies).values({id:companyId,name:"Warm fixture",issuePrefix:`W${companyId.slice(0,7)}`});
  await db.insert(agents).values({id:agentId,companyId,name:"Codex",adapterType:"paperclip_runner"});
  await db.insert(issues).values({id:issueId,companyId,title:"Warm task",status:"in_progress",assigneeAgentId:agentId});
  await db.insert(heartbeatRuns).values({id:runId,companyId,agentId,nativeIssueId:issueId,invocationSource:"manual",status:"running"});
  const [environment]=await db.insert(environments).values({id:environmentId,name:environmentId,driver:"plugin",status:"active",
   config:{pluginKey:pluginId,driverKey:"tasks",driverConfig:{},runnerLifecycleMode:"warm",runnerIdleTimeoutMs:300000}}).returning();
  await db.insert(plugins).values({id:pluginId,pluginKey:pluginId,packageName:"fixture-warm",version:"1.0.0",status:"ready",manifestJson:{
   capabilities:["environment.drivers.register"],environmentDrivers:[{driverKey:"tasks",supportsTasks:true,warmTaskVersion:1}],
  } as never});
  const workers={isRunning:()=>true,getWorker:()=>({supportedMethods:["environmentTask","environmentAcquireLease","environmentResumeLease","environmentReleaseLease"]}),
   call:async(_id:string,method:string,p:any)=>{
    calls.push(method==="environmentTask" ? p.operation.kind : method);
    if(method==="environmentAcquireLease") return {providerLeaseId:`task-${++allocation}`,metadata:{runId:p.runId}};
    if(method==="environmentResumeLease") {if(outage) throw Error("unreachable");
     if(resumeGate){if(++resumes===2)releaseResume!();await resumeGate;}return {providerLeaseId:stopped?null:p.providerLeaseId,metadata:{runId:p.runId}};}
    if(method==="environmentReleaseLease") return {providerLeaseId:p.providerLeaseId,state:"stopped"};
    if(p.operation.kind==="status") return {kind:"status",taskId:p.taskId,phase:stopped?"completed":"running",executionStopped:stopped,sessionStateAvailable:stopped};
    if(p.operation.kind==="stop") stopped=true;
    if(p.operation.kind==="prepare") return {kind:"prepared",taskId:p.taskId,launch:{configurationId:"configuration",requirementsDigest:`sha256:${"a".repeat(64)}`,provider:{}}};
    if(p.operation.kind==="check_project_auth") return {kind:"project_auth",taskId:p.taskId,configurationId:p.operation.configurationId,provider:"openai",allowed:true};
    if(p.operation.kind==="describe") return {kind:"description",taskId:p.taskId,runner:{version:"fixture",digest:`sha256:${"a".repeat(64)}`},warmTaskVersion:1};
    return {kind:"accepted",taskId:p.taskId};
   }} as unknown as PluginWorkerManager;
  const runtime=()=>environmentRuntimeService(db,{pluginWorkerManager:workers});
  const acquire=()=>runtime().acquireRunLease({companyId,environment:environment as never,issueId,agentId,heartbeatRunId:runId,persistedExecutionWorkspace:null,adapterType:"paperclip_runner"});
  const first=await acquire();
  const identity={runnerInstanceId:"runner",environmentLeaseId:first.lease.id,runId,normalizedSessionId:"session",turnId:"turn",itemId:"item"};
  await runtime().task({companyId,leaseId:first.lease.id,operation:{kind:"check_project_auth",configurationId:"configuration",provider:"openai",projectIds:[]}});
  await runtime().task({companyId,leaseId:first.lease.id,operation:{kind:"submit",preparedLaunchId:"configuration",projectIds:[],bootstrapTicket:"synthetic",accountIdentity:"account",
   runner:{harness:"codex",protocolMin:1,protocolMax:3,runnerId:identity.runnerInstanceId,leaseId:identity.environmentLeaseId,runId,sessionId:identity.normalizedSessionId,turnId:identity.turnId,itemId:identity.itemId},
   launch:{runnerVersion:"fixture",runnerDigest:`sha256:${"a".repeat(64)}`,maxOutboxBytes:262144,p0ReserveBytes:65536,lifecyclePolicy:{mode:"warm",idleTimeoutMs:300000}}}});
  await runtime().releaseRunLeases(runId,"released",undefined,"keep_running");
  await db.update(heartbeatRuns).set({status:"succeeded"}).where(eq(heartbeatRuns.id,runId));
  return {companyId,first,identity,calls,runtime,acquire,
   async nextRun(){runId=randomUUID();await db.insert(heartbeatRuns).values({id:runId,companyId,agentId,nativeIssueId:issueId,invocationSource:"manual",status:"running"});return runId;},
   stopped(){stopped=true;},outage(){outage=true;},
   raceResume(){resumeGate=new Promise<void>(resolve=>{releaseResume=resolve;});}};
 }
 it("hands a retained process to a new run after controller reconstruction",async()=>{
  const f=await fixture();
  expect(f.first.lease.leasePolicy).toBe("reuse_by_environment");
  expect(f.calls).not.toContain("stop");expect(f.calls).not.toContain("environmentReleaseLease");
  const runId=await f.nextRun(),next=await f.acquire();
  expect(next.lease.providerLeaseId).toBe(f.first.lease.providerLeaseId);
  expect(next.lease.id).not.toBe(f.first.lease.id);
  const identity={...f.identity,runId,turnId:"next-turn",itemId:"next-item"};
  const operation={kind:"prepare" as const,projectIds:[],priorIdentity:f.identity,accountIdentity:"account",requirements:{
   schema:"paperclip.runner.launch-requirements.v1" as const,identity,provider:"codex" as const,model:null,instructions:"Current run",runtimeContext:null,settings:{},approvalPolicy:"never" as const,readOnly:false,preparedConversation:false,includeCollaborationModeInstructions:false}};
  await expect(f.runtime().task({companyId:f.companyId,leaseId:next.lease.id,operation})).resolves.toMatchObject({kind:"prepared"});
  await expect(f.runtime().task({companyId:f.companyId,leaseId:next.lease.id,operation})).resolves.toMatchObject({kind:"prepared"});
  await expect(f.runtime().task({companyId:f.companyId,leaseId:next.lease.id,operation:{...operation,priorIdentity:{...f.identity,itemId:"foreign"}}})).rejects.toThrow("prior identity mismatch");
  expect(f.calls.filter(call=>call==="submit")).toHaveLength(1);
  const [old]=await db.select().from(environmentLeases).where(eq(environmentLeases.id,f.first.lease.id));
  expect(old.status).toBe("expired");
 });
 it("resumes stopped checkpoints through a new physical task instead of reusing a tombstone",async()=>{
  const f=await fixture();f.stopped();await f.nextRun();
  const next=await f.acquire();
  expect(next.lease.providerLeaseId).not.toBe(f.first.lease.providerLeaseId);
  expect(f.calls).toContain("complete");
  await expect(resolvePluginTaskResume(db,{companyId:f.companyId,leaseId:next.lease.id,priorIdentity:f.identity})).resolves.toBe(f.first.lease.providerLeaseId);
 });
 it("does not allocate a replacement when the retained computer cannot be inspected",async()=>{
  const f=await fixture();f.outage();await f.nextRun();
  await expect(f.acquire()).rejects.toThrow("unreachable");
  expect(f.calls.filter(call=>call==="environmentAcquireLease")).toHaveLength(1);
 });
 it("atomically admits only one successor to a retained task",async()=>{
  const f=await fixture();await f.nextRun();f.raceResume();
  const results=await Promise.allSettled([f.acquire(),f.acquire()]);
  expect(results.filter(result=>result.status==="fulfilled")).toHaveLength(1);
  expect(results.filter(result=>result.status==="rejected")).toHaveLength(1);
  expect(f.calls.filter(call=>call==="environmentAcquireLease")).toHaveLength(1);
 });
 it("replaces a task that stops after lease handoff before preparing the next run",async()=>{
  const f=await fixture();await f.nextRun();const next=await f.acquire();f.stopped();
  const result=await f.runtime().task({companyId:f.companyId,leaseId:next.lease.id,operation:{kind:"describe"}});
  expect(result.taskId).not.toBe(f.first.lease.providerLeaseId);
  await expect(resolvePluginTaskResume(db,{companyId:f.companyId,leaseId:next.lease.id,priorIdentity:f.identity})).resolves.toBe(f.first.lease.providerLeaseId);
 });

});
