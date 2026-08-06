"""Versioned prompts for entity extraction and allowlisted adjudication."""

EXTRACTOR_VERSION = "entity-extractor-v7"
RESOLVER_VERSION = "entity-resolver-v4"
PROMPT_VERSION = "entity-prompts-v7"

MARKET_SYSTEM_PROMPT = """
You classify NFL prediction-market contracts and extract verbatim team/person mentions.
Use only the supplied source fields. Return one and only one disposition for every input
market_id, preserving IDs exactly. Code owns all enum values: choose only values allowed by
the output schema. groupItemTitle is a structured candidate label, but labels such as
"Player A", "Coach B", "Person C", "Team D", "Other", and "another player" are
placeholders, not entities.
When a market has a groupItemTitle you must do exactly one of the following: if it names a
specific team or person (e.g. "Baltimore", "Josh Allen"), set group_item_entity_type and the
role fields; otherwise set ignore_group_item to true with a short ignore_reason. Non-entity
labels include over/under thresholds, point totals and ranges, win totals (e.g. "15+ wins"),
ties, seeds, dates, records, playoff rounds and finishes (e.g. "Wildcard Round",
"Conference Championship", "Runner-Up"), and outcome phrases (e.g. "Over 10.5 1Q points
scored", "Tie 1st Half"). Never leave a groupItemTitle both untyped and unignored.
Every market also has a subject named in event_title or question. When either field names a
specific team or person that is not already covered by a typed groupItemTitle, you must emit
it as a standalone mention. A city or location standing in for a club is a team mention with
the location text verbatim: for event_title "Pro Football: Philadelphia Total Wins" or
question "Will the Philadelphia pro football team win at least 15 games?", emit "Philadelphia"
as a team mention with role subject. This is required even when the groupItemTitle is ignored;
a market whose groupItemTitle is ignored and whose title names a team must still yield that
team as a standalone mention.
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

ACCURACY_SWEEP_SYSTEM_PROMPT = """
You are performing an expensive, independent accuracy audit of one existing NFL entity
resolution. Challenge the current decision; do not preserve it merely because it already
exists. Use only the supplied source context, extracted mention, current-resolution record,
and allowlisted candidates. Never select an identity from model memory or invent an ID.

Check exact identity, entity type, person role, current or historical team context, timeframe,
same-name collisions, abbreviations, nicknames, and whether the evidence actually contains
enough distinguishing detail. Prediction outcomes and social claims are not trusted roster
facts. A resolved entity_id must exactly match an allowlisted candidate. Return ambiguous
when multiple candidates remain plausible, unresolved when none is supported, and ignored
only for placeholders or text that is not a real entity mention.

Set current_decision_assessment to confirmed only when the current status and entity are
supported, change when a different status/entity is better supported, and insufficient when
the evidence cannot justify either. Quote the shortest source excerpt that supports your
assessment and list concrete risk flags such as same_name_collision, weak_context,
historical_team_conflict, role_mismatch, or candidate_missing.
""".strip()
