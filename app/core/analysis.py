from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from math import isfinite
from threading import Lock
from typing import Optional
from urllib import error as urlerror
from urllib import request as urlrequest

from app.core.config import settings
from app.crud.trades import compute_profit_holding
from app.db.models import Trade
from app.schemas.analysis import (
    AnalysisDataSufficiencyRead,
    AnalysisMarketStatRead,
    AnalysisStatsRead,
    AnalysisSummaryRead,
    AnalysisTagStatRead,
)

MIN_CLOSED_TRADES_FOR_AI = 5
_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, AnalysisSummaryRead]] = {}


@dataclass
class ClosedTradeSnapshot:
    trade_id: int
    market: str
    symbol: str
    name: Optional[str]
    opened_at: str
    closed_at: str
    profit_currency: str
    profit_value: float
    roi_pct: Optional[float]
    holding_days: int
    rating: Optional[int]
    tags: list[str]
    notes_buy: str
    notes_sell: str
    notes_review: str
    review_done: bool


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_tags(tags: Optional[str]) -> list[str]:
    return [part.strip() for part in str(tags or "").split(",") if part.strip()]


def _safe_float(value: object) -> Optional[float]:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if isfinite(n) else None


def _closed_trade_snapshot(trade: Trade) -> Optional[ClosedTradeSnapshot]:
    fills = {fill.side: fill for fill in trade.fills}
    buy = fills.get("buy")
    sell = fills.get("sell")
    if buy is None or sell is None:
        return None

    profit_value, holding_days = compute_profit_holding(buy, sell)
    principal = (float(buy.price) * float(buy.qty)) + float(buy.fee or 0)
    roi_pct = ((profit_value / principal) * 100.0) if principal > 0 else None
    return ClosedTradeSnapshot(
        trade_id=int(trade.id),
        market=str(trade.market),
        symbol=str(trade.symbol),
        name=(str(trade.name).strip() or None) if trade.name else None,
        opened_at=str(trade.opened_at),
        closed_at=str(trade.closed_at),
        profit_currency="JPY" if trade.market == "JP" else "USD",
        profit_value=float(profit_value),
        roi_pct=roi_pct,
        holding_days=int(holding_days),
        rating=int(trade.rating) if trade.rating is not None else None,
        tags=_parse_tags(trade.tags),
        notes_buy=str(trade.notes_buy or "").strip(),
        notes_sell=str(trade.notes_sell or "").strip(),
        notes_review=str(trade.notes_review or "").strip(),
        review_done=bool(trade.review_done),
    )


def _avg(values: list[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _round_or_none(value: Optional[float], digits: int = 1) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


def _build_stats(trades: list[Trade]) -> tuple[AnalysisStatsRead, list[ClosedTradeSnapshot]]:
    closed = [snapshot for trade in trades if (snapshot := _closed_trade_snapshot(trade)) is not None]
    open_trade_count = max(0, len(trades) - len(closed))

    wins = [item for item in closed if item.profit_value > 0]
    losses = [item for item in closed if item.profit_value < 0]
    breakeven = [item for item in closed if item.profit_value == 0]
    review_done_count = len([item for item in closed if item.review_done])
    roi_values = [item.roi_pct for item in closed if item.roi_pct is not None]
    holding_values = [float(item.holding_days) for item in closed]
    rating_values = [float(item.rating) for item in closed if item.rating is not None]

    tag_counter = Counter()
    for item in closed:
        tag_counter.update(item.tags)

    market_stats = []
    for market in ("JP", "US"):
        market_closed = [item for item in closed if item.market == market]
        market_wins = len([item for item in market_closed if item.profit_value > 0])
        market_losses = len([item for item in market_closed if item.profit_value < 0])
        market_even = len([item for item in market_closed if item.profit_value == 0])
        win_rate_pct = (market_wins / len(market_closed) * 100.0) if market_closed else None
        market_stats.append(
            AnalysisMarketStatRead(
                market=market,
                closed_trade_count=len(market_closed),
                win_trade_count=market_wins,
                loss_trade_count=market_losses,
                breakeven_trade_count=market_even,
                win_rate_pct=_round_or_none(win_rate_pct),
            )
        )

    stats = AnalysisStatsRead(
        closed_trade_count=len(closed),
        open_trade_count=open_trade_count,
        win_trade_count=len(wins),
        loss_trade_count=len(losses),
        breakeven_trade_count=len(breakeven),
        win_rate_pct=_round_or_none((len(wins) / len(closed) * 100.0) if closed else None),
        avg_roi_pct=_round_or_none(_avg([float(v) for v in roi_values])),
        avg_holding_days=_round_or_none(_avg(holding_values)),
        avg_rating=_round_or_none(_avg(rating_values)),
        review_completion_rate_pct=_round_or_none((review_done_count / len(closed) * 100.0) if closed else None),
        top_tags=[AnalysisTagStatRead(tag=tag, count=count) for tag, count in tag_counter.most_common(5)],
        market_breakdown=market_stats,
    )
    return stats, closed


def _trade_signature(trades: list[Trade]) -> str:
    base = [
        {
            "id": int(trade.id),
            "updated_at": str(trade.updated_at or ""),
            "closed_at": str(trade.closed_at or ""),
            "review_done": bool(trade.review_done),
        }
        for trade in trades
    ]
    return hashlib.sha1(json.dumps(base, sort_keys=True).encode("utf-8")).hexdigest()


def _safe_excerpt(text: str, limit: int = 160) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _representative_examples(trades: list[ClosedTradeSnapshot], reverse: bool) -> list[dict]:
    sorted_items = sorted(
        trades,
        key=lambda item: ((item.roi_pct if item.roi_pct is not None else -9999.0), item.profit_value),
        reverse=reverse,
    )
    examples = []
    for item in sorted_items[:3]:
        examples.append(
            {
                "market": item.market,
                "symbol": item.symbol,
                "name": item.name,
                "opened_at": item.opened_at,
                "closed_at": item.closed_at,
                "profit_currency": item.profit_currency,
                "profit_value": round(item.profit_value, 2),
                "roi_pct": _round_or_none(item.roi_pct),
                "holding_days": item.holding_days,
                "rating": item.rating,
                "tags": item.tags,
                "notes_buy": _safe_excerpt(item.notes_buy),
                "notes_sell": _safe_excerpt(item.notes_sell),
                "notes_review": _safe_excerpt(item.notes_review),
            }
        )
    return examples


def _llm_enabled() -> bool:
    return bool(str(settings.openai_api_key or "").strip())


def _mock_enabled() -> bool:
    return bool(settings.analysis_mock_enabled)


def _analysis_prompt_payload(stats: AnalysisStatsRead, closed: list[ClosedTradeSnapshot]) -> dict:
    wins = [item for item in closed if item.profit_value > 0]
    losses = [item for item in closed if item.profit_value < 0]
    return {
        "policy": {
            "language": "ja",
            "purpose": "自己分析支援",
            "forbidden": ["個別銘柄の推奨", "売買の推奨", "価格予測", "投資助言"],
        },
        "stats": json.loads(stats.model_dump_json()),
        "wins_examples": _representative_examples(wins, reverse=True),
        "loss_examples": _representative_examples(losses, reverse=False),
    }


def _top_tags_label(items: list[ClosedTradeSnapshot]) -> str:
    counter = Counter()
    for item in items:
        counter.update(item.tags)
    top = [tag for tag, _count in counter.most_common(2)]
    return " / ".join(top) if top else "タグ傾向はまだ薄めです"


def _missing_review_fields(closed: list[ClosedTradeSnapshot]) -> list[tuple[str, int]]:
    checks = {
        "購入理由": 0,
        "売却理由": 0,
        "考察": 0,
        "タグ": 0,
        "評価": 0,
    }
    for item in closed:
        if not item.notes_buy:
            checks["購入理由"] += 1
        if not item.notes_sell:
            checks["売却理由"] += 1
        if not item.notes_review:
            checks["考察"] += 1
        if not item.tags:
            checks["タグ"] += 1
        if item.rating is None or item.rating <= 0:
            checks["評価"] += 1
    return sorted(checks.items(), key=lambda kv: (-kv[1], kv[0]))


def _holding_tendency_label(wins: list[ClosedTradeSnapshot], losses: list[ClosedTradeSnapshot]) -> str:
    win_avg = _avg([float(item.holding_days) for item in wins]) if wins else None
    loss_avg = _avg([float(item.holding_days) for item in losses]) if losses else None
    if win_avg is None and loss_avg is None:
        return "保有日数の傾向はまだ十分に判断できません。"
    if win_avg is not None and loss_avg is None:
        return f"利益トレードの平均保有日数は {win_avg:.1f} 日です。"
    if win_avg is None and loss_avg is not None:
        return f"損失トレードの平均保有日数は {loss_avg:.1f} 日です。"
    if win_avg > loss_avg:
        return f"利益トレードは損失トレードより平均 {win_avg - loss_avg:.1f} 日長く保有しています。"
    if loss_avg > win_avg:
        return f"損失トレードは利益トレードより平均 {loss_avg - win_avg:.1f} 日長く保有しています。"
    return "利益・損失トレードで平均保有日数に大きな差はありません。"


def _build_rule_based_sections(stats: AnalysisStatsRead, closed: list[ClosedTradeSnapshot]) -> tuple[str, list[str], list[str], list[str]]:
    wins = [item for item in closed if item.profit_value > 0]
    losses = [item for item in closed if item.profit_value < 0]
    top_missing = _missing_review_fields(closed)
    top_gap_label = " / ".join(f"{label} {count}件" for label, count in top_missing[:3] if count > 0)
    top_gap_summary = top_gap_label or "主要な入力欠損は少なめです"
    summary = (
        f"決済済みトレードは {stats.closed_trade_count} 件、勝率は {stats.win_rate_pct or 0:.1f}% です。"
        f" 平均保有日数は {stats.avg_holding_days or 0:.1f} 日、レビュー完了率は {stats.review_completion_rate_pct or 0:.1f}% です。"
        f" 現時点では {top_gap_summary} が次の改善余地として見えます。"
    )

    win_patterns: list[str] = []
    if wins:
        win_patterns.append(
            f"利益トレード {len(wins)} 件では、タグ傾向として {_top_tags_label(wins)} が目立ちます。"
        )
        avg_roi_wins = _avg([float(item.roi_pct) for item in wins if item.roi_pct is not None])
        if avg_roi_wins is not None:
            win_patterns.append(f"利益トレードの平均損益率は {avg_roi_wins:.1f}% です。")
    win_patterns.append(_holding_tendency_label(wins, losses))

    loss_patterns: list[str] = []
    if losses:
        loss_patterns.append(
            f"損失トレード {len(losses)} 件では、タグ傾向として {_top_tags_label(losses)} が目立ちます。"
        )
        avg_roi_losses = _avg([float(item.roi_pct) for item in losses if item.roi_pct is not None])
        if avg_roi_losses is not None:
            loss_patterns.append(f"損失トレードの平均損益率は {avg_roi_losses:.1f}% です。")
    else:
        loss_patterns.append("損失トレードがまだ少ないため、負けパターンは十分に観測されていません。")
    if top_missing and top_missing[0][1] > 0:
        loss_patterns.append(f"決済後の振り返りでは {top_missing[0][0]} の未入力が最も多く、分析精度を下げています。")

    actions: list[str] = []
    for label, count in top_missing[:3]:
        if count <= 0:
            continue
        actions.append(f"次回の振り返りでは {label} を優先して埋めてください（未入力 {count} 件）。")
    if not actions:
        actions.append("次の5件は同じ基準でタグと考察を揃え、再現性を比較できるようにしてください。")
    if stats.review_completion_rate_pct is not None and stats.review_completion_rate_pct < 70:
        actions.append("レビュー完了率が低めなので、まず未レビューの決済済みトレードを片付けてください。")
    actions.append("勝ちトレードと負けトレードで保有日数とタグの差を見比べ、次回の売買ルール候補を1つ残してください。")

    return summary, win_patterns[:3], loss_patterns[:3], actions[:3]


def _extract_response_text(payload: dict) -> str:
    text = str(payload.get("output_text") or "").strip()
    if text:
        return text

    for item in payload.get("output") or []:
        for content in item.get("content") or []:
            if content.get("type") == "output_text":
                value = str(content.get("text") or "").strip()
                if value:
                    return value
    return ""


def _build_mock_sections(stats: AnalysisStatsRead) -> tuple[str, list[str], list[str], list[str]]:
    top_tags = [item.tag for item in stats.top_tags[:2]]
    top_tags_label = " / ".join(top_tags) if top_tags else "タグ傾向はまだ薄めです"
    summary = (
        f"テスト用のAI要約です。決済済みトレードは {stats.closed_trade_count} 件で、"
        f"勝率は {stats.win_rate_pct or 0:.1f}% です。現在は {top_tags_label} の記録が目立ちます。"
    )
    win_patterns = [
        "利益トレードの前提条件が記録されているかを確認してください。",
        f"平均保有日数は {stats.avg_holding_days or 0:.1f} 日で推移しています。",
        "勝ちトレードのタグやレビューを再現可能な形で揃えると比較しやすくなります。",
    ]
    loss_patterns = [
        "損失トレードでも売却理由と考察が残っているかを確認してください。",
        "レビュー未完了の決済済みトレードが多い場合、傾向の精度が落ちやすくなります。",
        "勝ち負け双方で同じ条件が混ざっていないか、タグの粒度を見直してください。",
    ]
    actions = [
        "次の5件はエントリー理由・売却理由・考察を必ず埋めて比較しやすくする。",
        "タグを2〜3種類の軸に絞って、勝ちパターンの偏りを見やすくする。",
        "レビュー完了率を上げて、統計と振り返りコメントの整合を確認する。",
    ]
    return summary, win_patterns, loss_patterns, actions


def _generate_llm_sections(stats: AnalysisStatsRead, closed: list[ClosedTradeSnapshot], user_key: str) -> tuple[str, list[str], list[str], list[str], str]:
    body = {
        "model": str(settings.openai_model or "gpt-4.1-mini").strip() or "gpt-4.1-mini",
        "store": False,
        "temperature": 0.2,
        "max_output_tokens": 700,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "tradetrace_analysis_summary",
                "strict": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "summary": {"type": "string"},
                        "win_patterns": {"type": "array", "items": {"type": "string"}, "maxItems": 3},
                        "loss_patterns": {"type": "array", "items": {"type": "string"}, "maxItems": 3},
                        "actions": {"type": "array", "items": {"type": "string"}, "maxItems": 3},
                    },
                    "required": ["summary", "win_patterns", "loss_patterns", "actions"],
                },
            }
        },
        "input": [
            {
                "role": "developer",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "あなたは TradeTrace の振り返りアシスタントです。"
                            "必ず日本語で、自己分析支援として簡潔にまとめてください。"
                            "投資助言は禁止です。個別銘柄の推奨、売買推奨、価格予測はしないでください。"
                            "与えられた集計と代表例だけを根拠にし、断定しすぎず、気づきと改善案に限定してください。"
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(_analysis_prompt_payload(stats, closed), ensure_ascii=False),
                    }
                ],
            },
        ],
    }
    payload = json.dumps(body).encode("utf-8")
    req = urlrequest.Request(
        url=str(settings.openai_base_url or "https://api.openai.com/v1/responses").strip(),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.openai_api_key}",
        },
        data=payload,
    )
    timeout_sec = max(1.0, float(settings.openai_timeout_ms) / 1000.0)
    try:
        with urlrequest.urlopen(req, timeout=timeout_sec) as res:
            response_body = json.loads(res.read().decode("utf-8"))
    except urlerror.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        detail = raw or f"http {exc.code}"
        raise RuntimeError(f"llm http error: {detail}") from exc
    except Exception as exc:  # pragma: no cover - exercised by monkeypatch in tests
        raise RuntimeError(f"llm request failed: {exc}") from exc

    parsed_text = _extract_response_text(response_body)
    if not parsed_text:
        raise RuntimeError("llm response text is empty")

    parsed = json.loads(parsed_text)
    summary = str(parsed.get("summary") or "").strip()
    win_patterns = [str(item).strip() for item in parsed.get("win_patterns") or [] if str(item).strip()]
    loss_patterns = [str(item).strip() for item in parsed.get("loss_patterns") or [] if str(item).strip()]
    actions = [str(item).strip() for item in parsed.get("actions") or [] if str(item).strip()]
    if not summary:
        raise RuntimeError("llm summary is empty")
    return summary, win_patterns[:3], loss_patterns[:3], actions[:3], user_key


def build_analysis_summary(trades: list[Trade], user_id: Optional[str]) -> AnalysisSummaryRead:
    stats, closed = _build_stats(trades)
    generated_at = _utc_now_iso()
    enough_data = stats.closed_trade_count >= MIN_CLOSED_TRADES_FOR_AI
    signature = _trade_signature(trades)
    cache_key = f"{user_id or 'public'}:{signature}"
    ttl_seconds = max(30, int(settings.analysis_cache_ttl_seconds))
    now_ts = datetime.now(timezone.utc).timestamp()

    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached and cached[0] > now_ts:
            return cached[1]

    rule_summary, rule_win_patterns, rule_loss_patterns, rule_actions = _build_rule_based_sections(stats, closed)

    if not enough_data:
        result = AnalysisSummaryRead(
            summary=rule_summary,
            win_patterns=rule_win_patterns,
            loss_patterns=rule_loss_patterns,
            actions=rule_actions,
            stats=stats,
            data_sufficiency=AnalysisDataSufficiencyRead(
                enough_data=False,
                minimum_closed_trade_count=MIN_CLOSED_TRADES_FOR_AI,
                closed_trade_count=stats.closed_trade_count,
                llm_status="rule_based",
                message=f"決済済みトレードが {MIN_CLOSED_TRADES_FOR_AI} 件未満のため、簡易分析を表示しています。",
            ),
            generated_at=generated_at,
        )
    elif _mock_enabled():
        summary, win_patterns, loss_patterns, actions = _build_mock_sections(stats)
        result = AnalysisSummaryRead(
            summary=summary,
            win_patterns=win_patterns,
            loss_patterns=loss_patterns,
            actions=actions,
            stats=stats,
            data_sufficiency=AnalysisDataSufficiencyRead(
                enough_data=True,
                minimum_closed_trade_count=MIN_CLOSED_TRADES_FOR_AI,
                closed_trade_count=stats.closed_trade_count,
                llm_status="mock",
                message="テスト用のAI要約を表示しています。本番APIはまだ使用していません。",
            ),
            generated_at=generated_at,
        )
    elif not _llm_enabled():
        result = AnalysisSummaryRead(
            summary=rule_summary,
            win_patterns=rule_win_patterns,
            loss_patterns=rule_loss_patterns,
            actions=rule_actions,
            stats=stats,
            data_sufficiency=AnalysisDataSufficiencyRead(
                enough_data=True,
                minimum_closed_trade_count=MIN_CLOSED_TRADES_FOR_AI,
                closed_trade_count=stats.closed_trade_count,
                llm_status="rule_based",
                message="ルールベース分析を表示しています。OpenAI要約はまだ未設定です。",
            ),
            generated_at=generated_at,
        )
    else:
        try:
            summary, win_patterns, loss_patterns, actions, _ = _generate_llm_sections(stats, closed, user_id or "public")
            result = AnalysisSummaryRead(
                summary=summary,
                win_patterns=win_patterns,
                loss_patterns=loss_patterns,
                actions=actions,
                stats=stats,
                data_sufficiency=AnalysisDataSufficiencyRead(
                    enough_data=True,
                    minimum_closed_trade_count=MIN_CLOSED_TRADES_FOR_AI,
                    closed_trade_count=stats.closed_trade_count,
                    llm_status="generated",
                    message="AI要約を表示しています。投資助言ではなく、自己分析支援です。",
                ),
                generated_at=generated_at,
            )
        except RuntimeError:
            result = AnalysisSummaryRead(
                summary=rule_summary,
                win_patterns=rule_win_patterns,
                loss_patterns=rule_loss_patterns,
                actions=rule_actions,
                stats=stats,
                data_sufficiency=AnalysisDataSufficiencyRead(
                    enough_data=True,
                    minimum_closed_trade_count=MIN_CLOSED_TRADES_FOR_AI,
                    closed_trade_count=stats.closed_trade_count,
                    llm_status="fallback",
                    message="AI要約の生成に失敗したため、ルールベース分析を表示しています。",
                ),
                generated_at=generated_at,
            )

    with _CACHE_LOCK:
        _CACHE[cache_key] = (now_ts + ttl_seconds, result)
    return result
