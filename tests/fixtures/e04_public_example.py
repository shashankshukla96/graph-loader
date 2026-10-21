"""Self-contained installed public example; producer is an owned local test double."""
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace

from neo4j_graph_loader import planning, schema

SCHEMA='''loading: {mode: bulk}
nodes:
- label: Person
  topic: people
  key_property: id
  properties: {id: {type: integer, required: true}}
edges:
- type: KNOWS
  topic: knows
  nodes: {source: Person, target: Person}
  source_key_property: id
  target_key_property: id
  properties: {weight: {type: integer}}
'''
class Producer:
    def __init__(self):self.pending=[];self.offset=0
    def produce(self,topic,*,value,partition,on_delivery):
        message=SimpleNamespace(topic=lambda:topic,partition=lambda:partition,offset=lambda n=self.offset:n)
        self.offset+=1;self.pending.append((on_delivery,message))
    def poll(self,timeout=0):
        pending,self.pending=self.pending,[]
        for callback,message in pending:callback(None,message)
    def flush(self,timeout=0):self.poll();return 0
with tempfile.TemporaryDirectory(prefix='e04-public-') as temporary:
    root=Path(temporary);path=root/'schema.yaml';path.write_text(SCHEMA)
    model=schema.load_schema(path);assert isinstance(model,schema.GraphSchema)
    try:schema.load_schema(root/'missing')
    except schema.SchemaLoadError as error:assert isinstance(error.__cause__,FileNotFoundError)
    else:raise AssertionError('missing schema succeeded')
    settings=planning.PlanSettings(topic='chunks',partitions=1,memory_mb=1,hub_degree=0)
    try:planning.producer_edge_planner(model,{'UNKNOWN':[]},settings,'unused')
    except planning.EdgePlanError:pass
    else:raise AssertionError('unknown source succeeded')
    planner=planning.producer_edge_planner(model,{'KNOWS':[{'source':{'id':1},'target':{'id':2},'properties':{'weight':1}}]},
        settings,'unused',context='inline',producer_factory=Producer)
    try:
        result=planner.finish(root/'plan.json')
        assert isinstance(result,planning.PlanResult) and isinstance(result.plan,planning.EdgePlan)
        assert result.plan.total_rows==1 and result.rows_read==1 and len(result.digest)==64
    finally:planner.close()
    signal=threading.Event();signal.set()
    cancelled=planning.producer_edge_planner(model,{},settings,'unused',context='inline',cancel_event=signal)
    try:cancelled.finish(root/'cancelled.json')
    except planning.EdgePlanCancelledError:pass
    else:raise AssertionError('cancelled planner succeeded')
    expired=planning.producer_edge_planner(model,{},settings,'unused',context='inline',timeout_seconds=1)
    expired._clock=lambda:0.;expired.start();expired._clock=lambda:2.
    try:expired.finish(root/'expired.json')
    except planning.EdgePlanTimeoutError:pass
    else:raise AssertionError('expired planner succeeded')
    assert not list(root.glob('*.lock')) and not list(root.glob('.edge-plan-spill-*'))
print(json.dumps({'public_schema':'success_and_error','public_planning':'one_row_result_and_input_error',
                  'cancellation':'approved_error','deadline':'approved_error','owned_resources':'reclaimed'}))
