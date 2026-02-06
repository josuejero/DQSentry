"""Rendering helpers for the publish helpers package."""

from __future__ import annotations

from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .constants import TEMPLATE_DIR


def render_scorecard(context: dict[str, Any]) -> str:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("scorecard.html.jinja")
    return template.render(context)
