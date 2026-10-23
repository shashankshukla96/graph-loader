#!/usr/bin/env python3
"""E06-only installed tiny load; owns new testcontainers, no developer settings."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import uuid

import docker
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.neo4j import Neo4jContainer
from testcontainers.core.network import Network
import yaml


class BadAdvertisedKafka(KafkaContainer):
    """Reachable bootstrap that advertises an unreachable container broker."""
    def tc_start(self):
        host=self.get_container_host_ip();port=self.get_exposed_port(self.port)
        script=(f"#!/bin/bash\n{self.boot_command}\n"
                f"export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{host}:{port},BROKER://e06-unreachable.invalid:9092\n"
                ". /etc/confluent/docker/bash-config\n/etc/confluent/docker/configure\n/etc/confluent/docker/launch\n")
        self.create_file(script.encode(),self.TC_START_SCRIPT)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python',type=Path,required=True,help='Noneditable installed interpreter outside checkout')
    parser.add_argument('--images',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    python=args.python.absolute();images=json.loads(args.images.read_text())['images']
    outside=Path(tempfile.mkdtemp(prefix='graph-loader-e06-service-cwd-'))
    assert subprocess.run(['git','rev-parse','--show-toplevel'],cwd=outside,capture_output=True).returncode==128
    token='e06'+uuid.uuid4().hex[:12];password=uuid.uuid4().hex
    database=token;prefix=token+'-group';commands=[];result={'token':token,'cwd':str(outside),'python':str(python),'images':images}
    # Deliberately construct public service settings; never read private files or
    # inherit developer service/credential configuration into the installed CLI.
    env={k:v for k,v in os.environ.items() if not k.startswith(('NEO4J','KAFKA','LOADER_','GRAPH_LOADER_','PYTHON'))}
    env.update({'GRAPH_LOADER_METRICS_ENABLED':'0','NEO4J_USERNAME':'neo4j','NEO4J_PASSWORD':password,'NEO4J_DATABASE':database})
    for row in images:env['GRAPH_LOADER_'+row['component'].upper()+'_IMAGE']=row['local_tag']
    client=docker.from_env();known=set();network=None

    def command(cmd,label,extra=None,expected=0,timeout=180):
        started=time.monotonic();expired=False
        with (out/(label+'.log')).open('w') as log:
            child=subprocess.Popen(list(map(str,cmd)),cwd=outside,env=env|(extra or {}),stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            try:code=child.wait(timeout=timeout)
            except subprocess.TimeoutExpired:expired=True;code=124
            finally:
                try:os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                child.wait(timeout=10)
        content=(out/(label+'.log')).read_text()
        assert password not in content and 'wrong-e06-secret' not in content, 'credential appeared in output'
        commands.append({'command':list(map(str,cmd)),'cwd':str(outside),'exit_code':code,'timeout':expired,
                         'seconds':time.monotonic()-started,'log':label+'.log','sha256':hashlib.sha256(content.encode()).hexdigest()})
        (out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
        assert not expired and code==expected,(label,code,content[-5000:])
        return content

    def owned_apps():
        attempts={p.name for p in out.rglob('graph-loader-*') if p.is_dir()}
        rows=[]
        for c in client.containers.list(all=True,filters={'label':'app=graph-loader'}):
            labels=c.labels
            run=labels.get('run_id','')
            if run==token or run.startswith(token+'.') or labels.get('attempt') in attempts:
                rows.append(c)
        return rows

    command([python,'-I','-c',"import pathlib,neo4j_graph_loader,importlib.metadata as m; p=pathlib.Path(neo4j_graph_loader.__file__); assert 'site-packages' in str(p); assert m.version('neo4j-graph-loader') == '0.1.0a1.dev0'; print(p)"],'installed-origin')
    try:
        with ExitStack() as stack:
            network=stack.enter_context(Network())
            neo=stack.enter_context(Neo4jContainer('neo4j:5.26-enterprise',password=password)
                .with_env('NEO4J_ACCEPT_LICENSE_AGREEMENT','yes')
                .with_network(network).with_network_aliases('neo4j'))
            kafka=stack.enter_context(KafkaContainer('confluentinc/cp-kafka:7.7.0')
                .with_network(network).with_network_aliases('kafka'))
            known={neo.get_wrapped_container().id,kafka.get_wrapped_container().id}
            env.update({'NEO4J_URI':neo.get_connection_url(),'LOADER_NEO4J_URI':'bolt://neo4j:7687',
                        'KAFKA_BOOTSTRAP_SERVERS':kafka.get_bootstrap_server(),'LOADER_KAFKA_BOOTSTRAP_SERVERS':'kafka:9092'})
            with GraphDatabase.driver(env['NEO4J_URI'],auth=('neo4j',password)) as driver:
                with driver.session(database='system') as session:session.run(f'CREATE DATABASE {database} WAIT').consume()
                schema={'loading':{'mode':'bulk','consumer_group_id':prefix,'unwind_batch_size':20,'edge_unwind_batch_size':20},
                        'nodes':[{'label':label,'topic':token+'-'+label.lower(),'key_property':'id',
                                  'properties':{'id':{'type':'integer','required':True,'constraint':{'type':'uniqueness'}}}}
                                 for label in ('Leaf','Hub')],
                        'edges':[{'type':'POINTS_AT','topic':token+'-edges','nodes':{'source':'Leaf','target':'Hub'},
                                  'source_key_property':'id','target_key_property':'id','properties':{'seq':{'type':'integer','required':True}}}]}
                config=outside/'schema.yaml';config.write_text(yaml.safe_dump(schema))
                topics=[n['topic'] for n in schema['nodes']]+[schema['edges'][0]['topic']]
                admin=AdminClient({'bootstrap.servers':env['KAFKA_BOOTSTRAP_SERVERS']})
                for future in admin.create_topics([NewTopic(t,1,1) for t in topics]).values():future.result(timeout=15)
                producer=Producer({'bootstrap.servers':env['KAFKA_BOOTSTRAP_SERVERS']})
                delivered=[]
                def ack(error,msg):
                    assert error is None,error
                    delivered.append((msg.topic(),msg.partition(),msg.offset()))
                for index in range(12):
                    producer.produce(topics[0],value=json.dumps({'id':index}),on_delivery=ack)
                    producer.produce(topics[2],value=json.dumps({'source':{'id':index},'target':{'id':0},'properties':{'seq':index}}),on_delivery=ack)
                producer.produce(topics[1],value=json.dumps({'id':0}),on_delivery=ack)
                assert producer.flush(15)==0 and len(delivered)==25
                entry=python.parent/'graph-loader'
                base=[entry,'start','--mode','bulk','--config',config,'--skip-image-build','--network',network.name,'--bulk-timeout-seconds','60']
                good=command(base+['--run-id',token,'--run-dir',out/'load','--plan-workers','1','--plan-writers','1','--plan-memory-mb','16','--keep-plan-topic'],'tiny-load')
                assert 'stage=planned-edges-complete' in good
                with driver.session(database=database) as session:
                    assert session.run('MATCH (n) RETURN count(n) AS n').single()['n']==13
                    rows=[dict(r) for r in session.run('MATCH (a:Leaf)-[r:POINTS_AT]->(b:Hub) RETURN a.id AS source,b.id AS target,r.seq AS seq ORDER BY source')]
                    assert rows==[{'source':i,'target':0,'seq':i} for i in range(12)]
                with driver.session(database='neo4j') as session:assert session.run('MATCH (n) RETURN count(n) AS n').single()['n']==0
                offsets=[]
                for label,topic,end in [('Leaf',topics[0],12),('Hub',topics[1],1)]:
                    consumer=Consumer({'bootstrap.servers':env['KAFKA_BOOTSTRAP_SERVERS'],'group.id':prefix+'-'+label})
                    try:offset=consumer.committed([TopicPartition(topic,0)],timeout=10)[0].offset
                    finally:consumer.close()
                    assert offset==end,(label,offset,end)
                    offsets.append({'label':label,'topic':topic,'committed':offset,'end':end})
                attempt=next((out/'load').glob('graph-loader-*'));plan=json.loads((attempt/'plans/edge-plan.json').read_text())
                assert plan['stats']['rows']==12 and plan['stats']['source']=='kafka'
                assert plan['stats']['boundary']=={topics[2]:[[0,0,12]]}
                ledger=json.loads((attempt/'plans/edge-plan.json.ledger.json').read_text())
                assert ledger['state']=='complete' and ledger['chunks']==len(plan['chunks']) and ledger['rows']==12
                assert len({row[0] for row in ledger['ledger']})==len(plan['chunks'])
                assert sum(row[2] for row in ledger['ledger'])==12
                assert not list((attempt/'spill').iterdir())
                assert (attempt/'timings/planned-summary.json').is_file()
                assert not owned_apps(),'successful load left owned application containers'
                result.update({'graph':rows,'node_count':13,'default_database_nodes':0,'offsets':offsets,'delivered_source_offsets':delivered,
                               'plan':plan,'ledger':ledger,'attempt':str(attempt),'installed_load':True,'service_versions':{'neo4j':'5.26-enterprise','kafka':'7.7.0'}})
                external=outside/'external-shared';external.mkdir()
                external_plan=external/'input-plan.json';external_plan.write_text(json.dumps(plan))
                sentinel=external/'foreign-sentinel';sentinel.write_text('retain')
                command(base+['--run-id',token,'--run-dir',out/'external','--edge-plan',external_plan,
                    '--plan-spill-dir',external,'--plan-writers','1'],'external-shared-plan',
                    {'GRAPH_LOADER_TIMING_DIR':str(external)})
                external_ledger=json.loads((external/'input-plan.json.ledger.json').read_text())
                assert external_ledger['state']=='complete' and external_ledger['rows']==12
                assert sentinel.read_text()=='retain' and not list(external.glob('.graph-loader-preflight-*'))
                result['external_plan_shared_spill_timings']={'path':str(external_plan),'ledger_rows':12,'sentinel_preserved':True}
                result['negatives']=[]
                for label,extra,needle in [
                    ('credentials',{'NEO4J_PASSWORD':'wrong-e06-secret'},'neo4j-auth'),
                    ('database',{'NEO4J_DATABASE':'e06MissingDatabase'},'neo4j-database'),
                    ('container-neo4j',{'LOADER_NEO4J_URI':'bolt://127.0.0.1:1'},'neo4j-connect'),
                    ('container-kafka',{'LOADER_KAFKA_BOOTSTRAP_SERVERS':'127.0.0.1:1'},'kafka-bootstrap')]:
                    text=command(base+['--run-id',token,'--run-dir',out/label],label,extra,expected=1,timeout=65)
                    assert needle in text and 'Traceback' not in text,(label,text[-3000:])
                    result['negatives'].append({'case':label,'phase':needle,'exit_code':1})
                text=command([*base,'--network',token+'-missing','--run-dir',out/'network'],'network',expected=1,timeout=30)
                assert 'network unavailable' in text and 'Traceback' not in text
                result['negatives'].append({'case':'network','exit_code':1})
                bad=stack.enter_context(BadAdvertisedKafka('confluentinc/cp-kafka:7.7.0')
                    .with_network(network).with_network_aliases('bad-kafka'))
                known.add(bad.get_wrapped_container().id)
                text=command(base+['--run-id',token,'--run-dir',out/'metadata'],'metadata',
                    {'LOADER_KAFKA_BOOTSTRAP_SERVERS':'bad-kafka:9092'},expected=1,timeout=65)
                assert 'kafka-metadata' in text and 'Traceback' not in text
                result['negatives'].append({'case':'advertised-metadata','exit_code':1,'phase':'kafka-metadata'})
                from docker.types import Mount
                absent=outside/'never-created-bind-input'
                try:
                    c=client.containers.create(next(r['id'] for r in images if r['component']=='node'),
                        entrypoint=['python','-c','pass'],mounts=[Mount('/proof',str(absent),type='bind')])
                except docker.errors.APIError:
                    assert not absent.exists()
                else:
                    c.remove(force=True)
                    raise AssertionError('missing bind unexpectedly accepted')
                result['negatives'].append({'case':'missing-daemon-bind','exit_code':'Docker API rejection','client_path_not_created':True})
                # Use the real installed coordinator launcher and host client;
                # a second container probes the restart-transparent DNS address.
                code='''import os,time,subprocess,json
from neo4j_graph_loader.orchestrator.docker_service import DockerService
from neo4j_graph_loader.loader.lease_client import BucketLeaseClient
from neo4j_graph_loader.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor
from neo4j_graph_loader.utils.schema_loader import load_schema
s=DockerService();s.prepare_images(['edge'],local_only=True)
c=s.run_bucket_lease_server(run_id=TOKEN,bucket_count=4,network=NETWORK)
try:
 host=s.bucket_lease_address(c,network=NETWORK,host=True)
 internal=s.bucket_lease_address(c,network=NETWORK,prefer_name=True)
 deadline=time.monotonic()+15
 while True:
  client=BucketLeaseClient(host,bucket_count=4,connect_timeout_seconds=1)
  try:
   if client.ping():break
  except Exception:
   if time.monotonic()>deadline:raise
  finally:client.close()
  time.sleep(.1)
 schema=load_schema(CONFIG)
 monitor=RelationshipBulkMonitor(schema.edges,TOKEN,{('POINTS_AT','0')},{('POINTS_AT','0'):c},
  bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],consumer_group_prefix=PREFIX,
  tracked_lease_server=c,docker_service=s,lease_network=NETWORK,lease_bucket_count=4,mode='stream')
 try:monitor.wait_for_lease_server_ready(10.0)
 finally:monitor.close()
 payload='from neo4j_graph_loader.loader.lease_client import BucketLeaseClient; c=BucketLeaseClient('+repr(internal)+',bucket_count=4); assert c.ping(); c.close()'
 result=s.client.containers.run(s._image('edge'),entrypoint=['python','-I','-c'],command=[payload],network=NETWORK,remove=True)
 print(json.dumps({'host':host,'container':internal,'host_and_container_ping':True,'actual_monitor_ready':True}))
finally:c.remove(force=True)
'''.replace('TOKEN',repr(token)).replace('NETWORK',repr(network.name)).replace('CONFIG',repr(str(config))).replace('PREFIX',repr(prefix))
                command([python,'-I','-c',code],'lease-addresses',timeout=45)
                result['lease_host_and_container']=True
                result['owned_application_cleanup']=not owned_apps()
            result['infrastructure_ids']=sorted(known)
            result['network_id']=network.id
        assert not client.containers.list(all=True,filters={'id':list(known)})
        result['infrastructure_removed']=True
        result['commands']=commands
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print('Installed tiny load, graph, offsets, database, lease host/container paths, negatives and cleanup passed')
    finally:
        for c in owned_apps():
            c.remove(force=True)
        if network is not None:
            try:network.remove()
            except docker.errors.NotFound:pass


if __name__=='__main__':
    main()
