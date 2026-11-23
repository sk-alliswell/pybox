"""Real agent-framework smoke using a deterministic model and no API calls."""
import importlib.metadata
import json
import os
import sys
import time
import traceback


def _build_graph():
    import operator
    from typing import Annotated, TypedDict
    from langgraph.graph import StateGraph, START, END
    from langgraph.checkpoint.memory import InMemorySaver

    class State(TypedDict):
        value: int
        visits: Annotated[list[str], operator.add]

    graph = StateGraph(State)
    graph.add_node('plan', lambda s: {'visits':['plan']})
    graph.add_node('tool', lambda s: {'value':s['value']*2,'visits':['tool']})
    graph.add_edge(START,'plan')
    graph.add_edge('plan','tool')
    graph.add_edge('tool',END)
    app = graph.compile(checkpointer=InMemorySaver())
    return app


def _build_agent():
    from deepagents import create_deep_agent
    from langchain_core.language_models.chat_models import BaseChatModel
    from langchain_core.messages import AIMessage, ToolMessage
    from langchain_core.outputs import ChatGeneration, ChatResult
    from langchain_core.tools import tool

    class ScriptedModel(BaseChatModel):
        @property
        def _llm_type(self):
            return 'offline-scripted'

        def bind_tools(self, tools, **kwargs):
            return self

        def _generate(self,messages,stop=None,run_manager=None,**kwargs):
            count = sum(isinstance(message,ToolMessage) for message in messages)
            actions = [('write_file',{'file_path':'/notes/demo.txt','content':'sandbox note'}),
                       ('read_file',{'file_path':'/notes/demo.txt'}),
                       ('add_numbers',{'a':2,'b':3})]
            if count<len(actions):
                name,args=actions[count]
                message=AIMessage(content='',tool_calls=[{'name':name,'args':args,'id':'call_'+str(count),'type':'tool_call'}])
            else:
                message=AIMessage(content='result=5')
            return ChatResult(generations=[ChatGeneration(message=message)])

    @tool
    def add_numbers(a:int,b:int)->int:
        """Add two integers deterministically."""
        return a+b

    agent=create_deep_agent(model=ScriptedModel(),tools=[add_numbers])
    return agent


_PREPARED={}


def prepare():
    """Construct empty runtime templates; never process a request here."""
    if 'graph' not in _PREPARED:
        _PREPARED['graph']=_build_graph()
    if 'agent' not in _PREPARED:
        _PREPARED['agent']=_build_agent()
    return {'checkpoint_threads':len(_PREPARED['graph'].checkpointer.storage),
            'agent_has_checkpointer':bool(_PREPARED['agent'].checkpointer)}


def _get_prepared():
    prepare()
    return _PREPARED


def langgraph_check():
    app=_get_prepared()['graph']
    config = {'configurable':{'thread_id':'sandbox-demo'}}
    first = app.invoke({'value':3,'visits':[]},config)
    second = app.invoke({'value':5},config)
    state = app.get_state(config).values
    histories = len(list(app.get_state_history(config)))
    checks = {'first_tool_result': first['value']==6,
        'second_tool_result': second['value']==10,
        'checkpoint_retained_between_invocations': second['visits']==['plan','tool','plan','tool'],
        'checkpoint_state_matches':state==second,'checkpoint_history':histories>=5}
    return {'checks':checks,'passed':all(checks.values()),'first':first,'second':second,'history_count':histories,
        'scope':'Real StateGraph nodes and in-guest memory checkpoint across invocations; not persistence across destroyed VMs'}


def deepagents_check():
    from langchain_core.messages import AIMessage, ToolMessage
    agent=_get_prepared()['agent']
    result=agent.invoke({'messages':[{'role':'user','content':'Write and read a note, then add 2 and 3.'}]},
                        config={'recursion_limit':20})
    messages=result['messages']
    outputs=[m for m in messages if isinstance(m,ToolMessage)]
    calls=[call['name'] for m in messages if isinstance(m,AIMessage) for call in m.tool_calls]
    files=result.get('files',{})
    data=files.get('/notes/demo.txt',{})
    content=data.get('content','')
    if isinstance(content,list):content='\n'.join(content)
    checks={'tool_sequence':calls==['write_file','read_file','add_numbers'],
        'three_tool_responses':len(outputs)==3,'all_tools_succeeded':all(getattr(m,'status','success')=='success' for m in outputs),
        'file_retained_in_agent_state':content=='sandbox note',
        'file_read_result':len(outputs)>1 and 'sandbox note' in str(outputs[1].content),
        'addition_result':len(outputs)>2 and str(outputs[2].content)=='5',
        'final_answer':messages[-1].content=='result=5'}
    return {'checks':checks,'passed':all(checks.values()),'tool_calls':calls,
        'tool_outputs':[{'name':m.name,'content':str(m.content)[:300]} for m in outputs],
        'files':list(files),'scope':'Real deepagents model/tool middleware and state-backed filesystem; scripted model, no external LLM or shell backend'}


def run(selection='all'):
    os.environ['LANGSMITH_TRACING']='false'
    began=time.perf_counter()
    results={'python':sys.version.split()[0],
        'versions':{n:importlib.metadata.version(n) for n in ('langgraph','deepagents','langchain-core','pydantic')},
        'uv_in_guest':os.path.exists('/usr/local/bin/uv')}
    for name, check in [('langgraph',langgraph_check),('deepagents',deepagents_check)]:
        if selection not in ('all',name):continue
        try:results[name]=check()
        except Exception as exc:
            results[name]={'passed':False,'error':repr(exc),'traceback':traceback.format_exc()[-2500:]}
    results['passed']=not results['uv_in_guest'] and all(value['passed'] for name,value in results.items() if name in ('langgraph','deepagents'))
    results['guest_elapsed_ms']=(time.perf_counter()-began)*1000
    print(json.dumps(results))
    if not results['passed']:raise RuntimeError('Agent harness gate failed')


if __name__=='__main__':
    run()
