"""Run a registered checklist agent and explicitly flush its graph state."""
import json
import os
import secrets
import time
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt


class Checklist(TypedDict, total=False):
    items: list[str]
    plan: list[str]
    result: dict


def plan_items(state):
    tasks = (" ".join(item.split()) for item in state["items"])
    unique = {}
    for task in tasks:
        if task:
            unique.setdefault(task.casefold(), task)
    return {"plan": list(unique.values())}


def finish(state):
    interrupt({"next": "finish_checklist"})
    return {"result": {"items": state["plan"], "count": len(state["plan"])}}


def run_agent():
    request = json.loads(os.environ.pop("PYBOX_AGENT_REQUEST"))
    run_id = request["run_id"]
    nonce = secrets.token_hex(16)
    session = CheckpointSession(request["binding"], request.get("memory"))
    graph = StateGraph(Checklist)
    graph.add_node("plan", plan_items)
    graph.add_node("finish", finish)
    graph.add_edge(START, "plan")
    graph.add_edge("plan", "finish")
    graph.add_edge("finish", END)
    agent = graph.compile(checkpointer=session.saver)
    config = {"configurable": {"thread_id": run_id}}
    if request["phase"] == "spawn":
        agent.invoke(request["input"], config)
        if agent.get_state(config).next != ("finish",):
            raise RuntimeError("agent failed to reach its checkpoint boundary")
        frame = {"kind": "paused", "run_id": run_id, "guest_nonce": nonce,
                 "memory": session.export()}
        print("PYBOX_AGENT_V1 " + json.dumps(frame, separators=(",", ":")), flush=True)
        # Stay alive until the controller durably commits the flush and stops the VM.
        time.sleep(60)
        raise RuntimeError("controller never finalized the checkpoint")
    result = agent.invoke(Command(resume=True), config)["result"]
    frame = {"kind": "completed", "run_id": run_id,
             "checkpoint_id": request["checkpoint_id"], "guest_nonce": nonce,
             "result": result}
    print("PYBOX_AGENT_V1 " + json.dumps(frame, separators=(",", ":")), flush=True)


run_agent()
