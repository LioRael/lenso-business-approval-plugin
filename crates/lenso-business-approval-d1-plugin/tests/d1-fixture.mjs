import { DatabaseSync } from "node:sqlite";
export function database(path=":memory:") {
 const db=new DatabaseSync(path),calls={batches:0,reads:0,sessions:[],failAfterCommit:false,malformedAfterCommit:false};
 const prepare=sql=>({bind(...args){this.args=args;return this;},args:[],async first(){calls.reads++;return db.prepare(sql).get(...this.args)??null;},async all(){calls.reads++;return {results:db.prepare(sql).all(...this.args)};},sql});
 const session={prepare,async batch(statements){calls.batches++;db.exec("BEGIN IMMEDIATE");try{const results=statements.map(stmt=>{if(/^SELECT/i.test(stmt.sql)){return {success:true,results:db.prepare(stmt.sql).all(...stmt.args),meta:{changes:0}};}const row=db.prepare(stmt.sql).run(...stmt.args);return {success:true,results:[],meta:{changes:Number(row.changes)}};});db.exec("COMMIT");if(calls.failAfterCommit){calls.failAfterCommit=false;throw new Error("lost_reply");}if(calls.malformedAfterCommit){calls.malformedAfterCommit=false;delete results[0].meta;}return results;}catch(error){if(db.isTransaction)db.exec("ROLLBACK");throw error;}}};
 return {withSession(consistency){calls.sessions.push(consistency);return session;},calls,close(){db.close();}};
}
export const scope=()=>({closed:false,async run(work){if(this.closed)throw new Error("event_closed");const result=await work();if(this.closed)throw new Error("event_closed");return result;}});
