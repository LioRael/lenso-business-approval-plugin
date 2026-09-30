const assertBatch = rows => { if (!Array.isArray(rows) || rows.length !== 2 || rows.some(row => row?.success !== true || !Array.isArray(row.results)) || !Number.isSafeInteger(rows[0]?.meta?.changes) || rows[0].meta.changes < 0 || rows[0].meta.changes > 1) throw new Error("approval_receipt_unknown"); };
const SCHEMA_VERSION=1;
const DDL=[
"CREATE TABLE IF NOT EXISTS lenso_approval_schema(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL)",
"CREATE TABLE IF NOT EXISTS business_approval_requests(request_id TEXT PRIMARY KEY,requester_instance TEXT NOT NULL,idempotency_key TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('pending','approved','rejected','cancelled','expired')),revision INTEGER NOT NULL CHECK(revision IN (1,2)),expires_us INTEGER NOT NULL,record_json TEXT NOT NULL,UNIQUE(requester_instance,idempotency_key))",
"INSERT INTO lenso_approval_schema(singleton,version) VALUES(1,1) ON CONFLICT(singleton) DO NOTHING",
];
export async function setup(database){const s=database.withSession("first-primary");const exists=await s.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").bind("lenso_approval_schema").first();if(!exists && await s.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").bind("business_approval_requests").first())throw new Error("approval_schema_unmanaged");if(exists && (await s.prepare("SELECT version FROM lenso_approval_schema WHERE singleton=1").first())?.version!==SCHEMA_VERSION)throw new Error("approval_schema_incompatible");await s.batch(DDL.map(q=>s.prepare(q)));if((await s.prepare("SELECT version FROM lenso_approval_schema WHERE singleton=1").first())?.version!==SCHEMA_VERSION)throw new Error("approval_schema_incompatible");}
export function create(database,scope,configuration){
 if(typeof database?.withSession!=="function"||typeof scope?.run!=="function"||configuration?.profile!=="workers-d1")throw new Error("invalid_approval_d1_facility");
 const run=f=>scope.run(()=>f(database.withSession("first-primary"))),decode=row=>row?JSON.parse(row.record_json):null;
 const transition=(kind,input)=>run(async(s)=>{
  const status=kind==="decide"?input.status:kind==="cancel"?"cancelled":"expired";
  if(kind==="decide"&&!['approved','rejected'].includes(status))throw new Error("invalid_approval_transition");
  const at=new Date().toISOString(),now=(BigInt(Date.now())*1000n).toString();
  const actor=kind==="expire"?null:input.actor,evidence=kind==="decide"?input.evidence:null,reason=kind==="expire"?null:input.reason;
  let constraint="",extra=[];
  if(kind==="cancel"){constraint=" AND requester_instance=?";extra=[input.caller];}
  if(kind==="expire"){constraint=" AND expires_us<=CAST(? AS INTEGER)";extra=[now];}
  const result=await s.batch([
   s.prepare(`UPDATE business_approval_requests SET status=?,revision=2,record_json=json_set(record_json,'$.status',?,'$.revision',2,'$.terminal_caller_instance',?,'$.terminal_actor',?,'$.evidence_ref',?,'$.reason',?,'$.terminal_at',?) WHERE request_id=? AND status='pending' AND revision=1${constraint}`).bind(status,status,input.caller,actor,evidence,reason,at,input.id,...extra),
   s.prepare("SELECT record_json FROM business_approval_requests WHERE request_id=?").bind(input.id),
  ]);
  assertBatch(result);
  return {changed:result[0].meta.changes===1,approval:decode(result[1]?.results?.[0])};
 });
 return Object.freeze({
  now_us:()=> (BigInt(Date.now())*1000n).toString(),
  readiness:()=>run(async(s)=>{if((await s.prepare("SELECT version FROM lenso_approval_schema WHERE singleton=1").first())?.version!==1)throw new Error("approval_setup_required");return true;}),
  request:input=>run(async(s)=>{
   const i=input.intent;const record={...i,status:"pending",revision:1,terminal_caller_instance:null,terminal_actor:null,evidence_ref:null,reason:null,terminal_at:null};
   const result=await s.batch([
    s.prepare("INSERT INTO business_approval_requests(request_id,requester_instance,idempotency_key,status,revision,expires_us,record_json) VALUES(?,?,?,'pending',1,CAST(? AS INTEGER),?) ON CONFLICT DO NOTHING").bind(i.request_id,i.requester_instance,i.idempotency_key,input.expires_us,JSON.stringify(record)),
    s.prepare("SELECT record_json FROM business_approval_requests WHERE request_id=? OR (requester_instance=? AND idempotency_key=?)").bind(i.request_id,i.requester_instance,i.idempotency_key),
    ]);assertBatch(result);return {created:result[0].meta.changes===1,rows:result[1].results.map(decode)};
  }),
  read:input=>run(async(s)=>decode(await s.prepare("SELECT record_json FROM business_approval_requests WHERE request_id=? AND (? IS NULL OR requester_instance=?)").bind(input.id,input.requester,input.requester).first())),
  decide:input=>transition("decide",input),cancel:input=>transition("cancel",input),expire:input=>transition("expire",input),
 });
}
