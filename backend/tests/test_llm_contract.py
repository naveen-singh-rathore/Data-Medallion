"""Guard: everything the pipeline sends to a model, pinned by hash.

Recorded LLM answers (fixtures/llm_cache.jsonl) are keyed by the exact request. A change to a prompt,
a tool description or a JSON schema (including a docstring on a Pydantic model behind a schema)
changes every request, so the recorded answers stop replaying. If the change is intended: re-record
in live mode, re-export the fixtures, and update the hash below.
"""

import hashlib
import json

from pipeline.agent_tools import tool_schemas
from pipeline.context import draft_schema
from pipeline.dq_agent.prompts import submit_rules_tool, system_prompt
from pipeline.silver import classify, mapping_assist

LLM_CONTRACT_SHA256 = "0b804c50f02042d9e56a6a717aa3c9d185fc173b16c57ac11c25f2644a7e101a"


def llm_contract() -> dict:
    return {
        "dq_agent_system": system_prompt(),
        "dq_agent_tools": tool_schemas(),
        "dq_agent_submit": submit_rules_tool(),
        "context_draft_schema": draft_schema(["a", "b"]),
        "classify_schema": classify._schema(["x", classify.OTHER]),
        "mapping_assist_schema": mapping_assist._schema(["c"]),
    }


def test_llm_facing_prompts_and_schemas_are_unchanged() -> None:
    digest = hashlib.sha256(json.dumps(llm_contract(), sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    assert digest == LLM_CONTRACT_SHA256, (
        "a prompt or schema sent to the model changed; recorded answers will no longer replay "
        "(re-record in live mode, export fixtures, then update LLM_CONTRACT_SHA256)"
    )
