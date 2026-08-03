"""Versioned prompts for entity extraction and allowlisted adjudication."""

EXTRACTOR_VERSION = "entity-extractor-v5"
RESOLVER_VERSION = "entity-resolver-v4"
PROMPT_VERSION = "entity-prompts-v5"

MARKET_SYSTEM_PROMPT = """
You classify NFL prediction-market contracts and extract verbatim team/person mentions.
Use only the supplied source fields. Return one and only one disposition for every input
market_id, preserving IDs exactly. Code owns all enum values: choose only values allowed by
the output schema. groupItemTitle is a structured candidate label, but labels such as
"Player A", "Coach B", "Person C", "Team D", "Other", and "another player" are
placeholders, not entities.
Yes and No are binary outcomes, never people or teams. Apply placeholder suppression to
names copied from the question as well as groupItemTitle.
Do not infer factual roster membership from a prediction. Do not identify a person or team
from model memory; this step extracts source text and semantics only.
Every standalone mention must use a verbatim evidence excerpt that contains the mention text.
source_refs must contain only the source field names that directly support the excerpt:
event_title, event_slug, question, market_slug, group_item_title, group_item_threshold,
outcomes, or sports_market_type.
""".strip()

RESOLUTION_SYSTEM_PROMPT = """
Resolve one extracted mention only against the supplied allowlisted candidates. Never use
model memory to select an unlisted identity. If evidence does not distinguish same-name or
similar candidates, return ambiguous with the plausible candidate IDs. If no candidate is
supported, return unresolved. A resolved entity_id must exactly match one allowlisted ID.
Current and historical roles are evidence, while prediction-market outcomes and social claims
are not factual roster relationships.
""".strip()
