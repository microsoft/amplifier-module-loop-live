"""Real context writes and loop delivery, using a model-free finite engine."""
import asyncio
from collections import deque
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from amplifier_module_loop_live.runtime import Input, Runtime
from amplifier_module_loop_live.orchestrator import BundleLiveOrchestrator, StreamingOrchestrator
from amplifier_module_loop_live.host import HostAdapter, AttachmentContext
from amplifier_module_loop_live.provenance import InputContext

class Context:
    def __init__(self):self.messages=[]
    async def add_message(self,message):self.messages.append(message)
    async def get_messages(self):return self.messages

async def test_boundary_steering_retains_service_provenance_and_user_quotes():
    loop=BundleLiveOrchestrator({});loop.runtime=Runtime();loop.host=HostAdapter();loop.coordinator=None
    command=Input('service','The worker finished.',source='amplifier-delegate',id='observation',call_id='call')
    quoted=Input('user',loop._text(command),id='user-quote')
    loop.pending=deque([command,quoted]);context=Context()
    await loop._drain_steering(context,SimpleNamespace(emit=AsyncMock()),1)
    assert context.messages[0]['role']=='user'
    assert context.messages[0]['content']==context.messages[1]['content']
    assert context.messages[0]['metadata']['amplifier_input']=={'version':1,'kind':'service','id':'observation','source':'amplifier-delegate','call_id':'call'}
    assert context.messages[1]['metadata']['amplifier_input']['kind']=='user'

async def test_new_turn_uses_tagged_context_before_engine_writes(monkeypatch):
    runtime=Runtime();context=Context();saved=asyncio.Event()
    command=Input('service','Worker completed',source='amplifier-delegate',id='new-turn')
    async def finite(self,prompt,ctx,*args):
        await ctx.add_message({'role':'user','content':prompt,'metadata':{'existing':'kept'}})
        saved.set();return 'done'
    monkeypatch.setattr(StreamingOrchestrator,'execute',finite)
    capabilities={'live.runtime':runtime}
    coordinator=SimpleNamespace(get_capability=capabilities.get,register_capability=lambda name,value:capabilities.__setitem__(name,value),config={})
    hooks=SimpleNamespace(register=lambda *a,**kw:lambda:None,emit=AsyncMock())
    loop=BundleLiveOrchestrator({});loop.root_provider=SimpleNamespace()
    await runtime.submit(command)
    running=asyncio.create_task(loop.execute('',context,{'fixture':loop.root_provider},{},hooks,coordinator))
    await asyncio.wait_for(saved.wait(),2)
    await runtime.submit(Input('stop'));await asyncio.wait_for(running,2)
    row=context.messages[0]
    assert row['role']=='user' and row['content']==loop._text(command)
    assert row['metadata']['existing']=='kept'
    assert row['metadata']['amplifier_input']['id']=='new-turn'

async def test_prompt_wrapper_tags_only_one_matching_row_and_keeps_attachments():
    context=Context();command=Input('user','caption',id='upload')
    content=[{'type':'text','text':'caption'},{'type':'image_url','image_url':{'url':'fixture'}}]
    tagged=InputContext(AttachmentContext(context,'caption',content),'caption',command)
    await tagged.add_message({'role':'assistant','content':'caption'})
    await tagged.add_message({'role':'user','content':'caption'})
    await tagged.add_message({'role':'user','content':'caption'})
    assert context.messages[1]['content']==content
    assert context.messages[1]['metadata']['amplifier_input']['id']=='upload'
    assert 'metadata' not in context.messages[0] and 'metadata' not in context.messages[2]
