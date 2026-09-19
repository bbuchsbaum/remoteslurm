"""Declarative, observation-first campaign workspaces."""

from .contracts import evaluate_output, stage_contract
from .manager import CampaignManager
from .model import CampaignDefinition, CampaignName, WorkUnit
from .spec import compile_campaign, load_campaign

__all__ = [
    "CampaignDefinition",
    "CampaignName",
    "CampaignManager",
    "WorkUnit",
    "compile_campaign",
    "evaluate_output",
    "load_campaign",
    "stage_contract",
]
