"""Parametrisierungs-Module: KG-Daten → Simulations-Schocks."""
from .s1_soja import S1Parametrizer, S1ParametrizerResult
from .s10_iran import S10Parametrizer, S10ParametrizerResult

__all__ = [
    "S1Parametrizer", "S1ParametrizerResult",
    "S10Parametrizer", "S10ParametrizerResult",
]
