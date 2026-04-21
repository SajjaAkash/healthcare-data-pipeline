from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineMetric:
    metric_name: str
    metric_value: float
    grain: str
    report_date: str


@dataclass(frozen=True)
class QualityResult:
    rule_name: str
    passed: bool
    detail: str
