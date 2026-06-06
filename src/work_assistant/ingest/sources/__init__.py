"""Source implementations.

Importing this package registers each source via side-effect.
"""

from work_assistant.ingest.registry import SOURCES
from work_assistant.ingest.sources.slack import SlackSource

SOURCES["slack"] = SlackSource
