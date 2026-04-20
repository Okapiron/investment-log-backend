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
    AnalysisDiagnosisCardRead,
    AnalysisHoldingBucketRead,
    AnalysisLatestImportRead,
    AnalysisMarketStatRead,
    AnalysisReviewGapRead,
    AnalysisStatsRead,
    AnalysisSummaryRead,
    AnalysisTagStatRead,
    AnalysisTopImprovementRead,
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
    holding_days: Optional[int]
    data_quality: str
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
    position_side = str(getattr(trade, "position_side", "long") or "long")
    open_fill = fills.get("buy" if position_side == "long" else "sell")
    close_fill = fills.get("sell" if position_side == "long" else "buy")
    if open_fill is None or close_fill is None:
        return None

    data_quality = str(getattr(trade, "data_quality", "full") or "full")
    if data_quality == "realized_only" and getattr(trade, "broker_profit_jpy", None) is not None:
        profit_value = float(trade.broker_profit_jpy)
        holding_days = None
    else:
        profit_value, holding_days = compute_profit_holding(open_fill, close_fill, position_side=position_side)
    principal = (float(open_fill.price) * float(open_fill.qty)) + float(open_fill.fee or 0)
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
        holding_days=int(holding_days) if holding_days is not None else None,
        data_quality=data_quality,
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


def _avg_abs(values: list[float]) -> Optional[float]:
    if not values:
        return None
    return sum(abs(value) for value in values) / len(values)


def _round_or_none(value: Optional[float], digits: int = 1) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


def _win_rate(trades: list[ClosedTradeSnapshot]) -> Optional[float]:
    if not trades:
        return None
    wins = len([item for item in trades if item.profit_value > 0])
    return (wins / len(trades)) * 100.0


def _streak_lengths(closed: list[ClosedTradeSnapshot]) -> tuple[int, int]:
    longest_win = 0
    longest_loss = 0
    current_win = 0
    current_loss = 0
    ordered = sorted(closed, key=lambda item: (item.closed_at, item.opened_at, item.trade_id))
    for item in ordered:
        if item.profit_value > 0:
            current_win += 1
            current_loss = 0
        elif item.profit_value < 0:
            current_loss += 1
            current_win = 0
        else:
            current_win = 0
            current_loss = 0
        longest_win = max(longest_win, current_win)
        longest_loss = max(longest_loss, current_loss)
    return longest_win, longest_loss


def _select_primary_market(closed: list[ClosedTradeSnapshot]) -> tuple[Optional[str], list[ClosedTradeSnapshot]]:
    if not closed:
        return None, []

    grouped: dict[str, list[ClosedTradeSnapshot]] = {}
    for item in closed:
        grouped.setdefault(item.market, []).append(item)

    primary_market = max(
        grouped.keys(),
        key=lambda market: (
            len(grouped[market]),
            _avg_abs([item.profit_value for item in grouped[market]]) or 0.0,
            market == "JP",
        ),
    )
    return primary_market, grouped[primary_market]


def _holding_buckets(primary_closed: list[ClosedTradeSnapshot]) -> list[AnalysisHoldingBucketRead]:
    ranges = [
        ("1-3日", lambda days: days <= 3),
        ("4-10日", lambda days: 4 <= days <= 10),
        ("11-30日", lambda days: 11 <= days <= 30),
        ("31日以上", lambda days: days >= 31),
    ]

    buckets: list[AnalysisHoldingBucketRead] = []
    for label, predicate in ranges:
        items = [item for item in primary_closed if item.holding_days is not None and predicate(item.holding_days)]
        wins = [item.profit_value for item in items if item.profit_value > 0]
        losses = [item.profit_value for item in items if item.profit_value < 0]
        buckets.append(
            AnalysisHoldingBucketRead(
                label=label,
                closed_trade_count=len(items),
                win_rate_pct=_round_or_none(_win_rate(items)),
                avg_net_profit_amount=_round_or_none(_avg([item.profit_value for item in items])),
                avg_win_profit_amount=_round_or_none(_avg(wins)),
                avg_loss_amount=_round_or_none(_avg_abs(losses)),
            )
        )
    return buckets


def _window_metrics(items: list[ClosedTradeSnapshot]) -> dict[str, object]:
    wins = [item.profit_value for item in items if item.profit_value > 0]
    losses = [item.profit_value for item in items if item.profit_value < 0]
    return {
        "closed_trade_count": len(items),
        "win_rate_pct": _round_or_none(_win_rate(items)),
        "avg_win_profit_amount": _round_or_none(_avg(wins)),
        "avg_loss_amount": _round_or_none(_avg_abs(losses)),
        "avg_holding_days": _round_or_none(_avg([float(item.holding_days) for item in items if item.holding_days is not None])),
        "avg_roi_pct": _round_or_none(_avg([float(item.roi_pct) for item in items if item.roi_pct is not None])),
    }


def _build_stats(trades: list[Trade]) -> tuple[AnalysisStatsRead, list[ClosedTradeSnapshot]]:
    closed = [snapshot for trade in trades if (snapshot := _closed_trade_snapshot(trade)) is not None]
    open_trade_count = max(0, len(trades) - len(closed))

    wins = [item for item in closed if item.profit_value > 0]
    losses = [item for item in closed if item.profit_value < 0]
    breakeven = [item for item in closed if item.profit_value == 0]
    realized_only_count = len([item for item in closed if item.data_quality == "realized_only"])
    holding_analysis_count = len([item for item in closed if item.holding_days is not None])
    review_done_count = len([item for item in closed if item.review_done])
    roi_values = [item.roi_pct for item in closed if item.roi_pct is not None]
    holding_values = [float(item.holding_days) for item in closed if item.holding_days is not None]
    rating_values = [float(item.rating) for item in closed if item.rating is not None]

    tag_counter = Counter()
    for item in closed:
        tag_counter.update(item.tags)

    primary_market, primary_closed = _select_primary_market(closed)
    primary_profit_currency = primary_closed[0].profit_currency if primary_closed else None
    primary_wins = [item for item in primary_closed if item.profit_value > 0]
    primary_losses = [item for item in primary_closed if item.profit_value < 0]
    avg_win_profit_amount = _avg([item.profit_value for item in primary_wins])
    avg_loss_amount = _avg_abs([item.profit_value for item in primary_losses])
    profit_loss_ratio = None
    if avg_win_profit_amount is not None and avg_loss_amount not in (None, 0):
        profit_loss_ratio = avg_win_profit_amount / avg_loss_amount

    recent_primary = sorted(primary_closed, key=lambda item: (item.closed_at, item.opened_at, item.trade_id))[-20:]
    recent_metrics = _window_metrics(recent_primary)
    longest_win_streak, longest_loss_streak = _streak_lengths(primary_closed)

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
        primary_market=primary_market,
        primary_profit_currency=primary_profit_currency,
        primary_closed_trade_count=len(primary_closed),
        realized_only_trade_count=realized_only_count,
        holding_analysis_trade_count=holding_analysis_count,
        avg_win_profit_amount=_round_or_none(avg_win_profit_amount),
        avg_loss_amount=_round_or_none(avg_loss_amount),
        profit_loss_ratio=_round_or_none(profit_loss_ratio),
        avg_win_holding_days=_round_or_none(_avg([float(item.holding_days) for item in primary_wins if item.holding_days is not None])),
        avg_loss_holding_days=_round_or_none(_avg([float(item.holding_days) for item in primary_losses if item.holding_days is not None])),
        recent_closed_trade_count=int(recent_metrics["closed_trade_count"]),
        recent_win_rate_pct=recent_metrics["win_rate_pct"],
        recent_avg_win_profit_amount=recent_metrics["avg_win_profit_amount"],
        recent_avg_loss_amount=recent_metrics["avg_loss_amount"],
        recent_avg_holding_days=recent_metrics["avg_holding_days"],
        recent_avg_roi_pct=recent_metrics["avg_roi_pct"],
        longest_win_streak=longest_win_streak,
        longest_loss_streak=longest_loss_streak,
        top_tags=[AnalysisTagStatRead(tag=tag, count=count) for tag, count in tag_counter.most_common(5)],
        market_breakdown=market_stats,
        holding_buckets=_holding_buckets(primary_closed),
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


def _latest_import_signature(latest_import: Optional[AnalysisLatestImportRead]) -> str:
    if latest_import is None:
        return "no-import"
    return f"{latest_import.broker}:{latest_import.imported_at}:{latest_import.created_count}:{latest_import.updated_count}:{latest_import.audit_gap_jpy}"


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
    win_avg = _avg([float(item.holding_days) for item in wins if item.holding_days is not None]) if wins else None
    loss_avg = _avg([float(item.holding_days) for item in losses if item.holding_days is not None]) if losses else None
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


def _market_basis_label(stats: AnalysisStatsRead) -> str:
    if not stats.primary_market or not stats.primary_closed_trade_count:
        return "決済済みトレード"
    currency = stats.primary_profit_currency or ""
    return f"{stats.primary_market}トレード {stats.primary_closed_trade_count}件（{currency}ベース）"


def _review_gap_reads(closed: list[ClosedTradeSnapshot]) -> list[AnalysisReviewGapRead]:
    return [
        AnalysisReviewGapRead(label=label, missing_count=count)
        for label, count in _missing_review_fields(closed)
        if count > 0
    ]


def _build_diagnosis_cards(stats: AnalysisStatsRead, closed: list[ClosedTradeSnapshot]) -> list[AnalysisDiagnosisCardRead]:
    basis_label = _market_basis_label(stats)
    cards: list[AnalysisDiagnosisCardRead] = []

    pnl_tone = "neutral"
    pnl_hypothesis = "収支構造はまだ判断材料が少ない状態です。"
    pnl_summary = f"{basis_label} を金額損益ベースで見ると、まだ勝ち負けの偏りを断言しにくいです。"
    if stats.avg_win_profit_amount is not None and stats.avg_loss_amount is not None:
        if (stats.profit_loss_ratio or 0) < 1:
            pnl_tone = "warning"
            if (stats.win_rate_pct or 0) >= 50:
                pnl_hypothesis = "勝率に対して1回の負けが重い可能性があります。"
            else:
                pnl_hypothesis = "大きな負けが全体収支を圧迫している可能性があります。"
        else:
            pnl_tone = "positive"
            if (stats.win_rate_pct or 0) < 50:
                pnl_hypothesis = "勝率が低めでも、利幅で補えている可能性があります。"
            else:
                pnl_hypothesis = "利益額と損失額のバランスは比較的安定しています。"
        pnl_summary = (
            f"{basis_label} では平均利益額が {stats.avg_win_profit_amount or 0:.1f}、"
            f"平均損失額が {stats.avg_loss_amount or 0:.1f} です。"
        )
    cards.append(
        AnalysisDiagnosisCardRead(
            key="pnl_structure",
            title="収支構造",
            hypothesis=pnl_hypothesis,
            summary=pnl_summary,
            evidence=[
                f"平均利益額: {stats.avg_win_profit_amount or 0:.1f}",
                f"平均損失額: {stats.avg_loss_amount or 0:.1f}",
                f"利益額/損失額比: {stats.profit_loss_ratio or 0:.2f} / 勝率 {stats.win_rate_pct or 0:.1f}%",
            ],
            tone=pnl_tone,
        )
    )

    best_bucket = max(
        stats.holding_buckets,
        key=lambda item: ((item.avg_net_profit_amount or -999999.0), item.closed_trade_count),
        default=None,
    )
    worst_bucket = min(
        [item for item in stats.holding_buckets if item.closed_trade_count > 0],
        key=lambda item: ((item.avg_net_profit_amount or 999999.0), -item.closed_trade_count),
        default=None,
    )
    holding_tone = "neutral"
    holding_hypothesis = "保有日数による大きな差はまだ断定できません。"
    if (
        stats.avg_win_holding_days is not None
        and stats.avg_loss_holding_days is not None
        and stats.avg_loss_holding_days > stats.avg_win_holding_days + 3
    ):
        holding_tone = "warning"
        holding_hypothesis = "長めの保有で損失が膨らみやすい傾向があります。"
    elif (
        stats.avg_win_holding_days is not None
        and stats.avg_loss_holding_days is not None
        and stats.avg_win_holding_days > stats.avg_loss_holding_days + 3
    ):
        holding_tone = "positive"
        holding_hypothesis = "少し保有できたトレードの方が利益につながりやすい傾向があります。"
    holding_summary = (
        f"勝ちトレードの平均保有日数は {stats.avg_win_holding_days or 0:.1f} 日、"
        f"負けトレードは {stats.avg_loss_holding_days or 0:.1f} 日です。"
    )
    bucket_evidence = "保有日数帯の特徴はまだ十分に出ていません。"
    if best_bucket and worst_bucket and best_bucket.closed_trade_count > 0 and worst_bucket.closed_trade_count > 0:
        bucket_evidence = (
            f"最も良い帯は {best_bucket.label}（平均損益 {best_bucket.avg_net_profit_amount or 0:.1f}）、"
            f"最も弱い帯は {worst_bucket.label}（平均損益 {worst_bucket.avg_net_profit_amount or 0:.1f}）です。"
        )
    cards.append(
        AnalysisDiagnosisCardRead(
            key="holding_execution",
            title="保有と執行の歪み",
            hypothesis=holding_hypothesis,
            summary=holding_summary,
            evidence=[
                f"勝ち平均保有日数: {stats.avg_win_holding_days or 0:.1f}日",
                f"負け平均保有日数: {stats.avg_loss_holding_days or 0:.1f}日",
                bucket_evidence,
            ],
            tone=holding_tone,
        )
    )

    recent_tone = "neutral"
    recent_hypothesis = "直近では大きな変化はまだ強く出ていません。"
    if stats.recent_closed_trade_count < 5:
        recent_summary = "直近比較の件数が少ないため、最近の変化は参考値です。"
    else:
        recent_summary = (
            f"直近{stats.recent_closed_trade_count}件では勝率 {stats.recent_win_rate_pct or 0:.1f}%、"
            f"平均損失額 {stats.recent_avg_loss_amount or 0:.1f} です。"
        )
        if (
            stats.avg_loss_amount is not None
            and stats.recent_avg_loss_amount is not None
            and stats.recent_avg_loss_amount > stats.avg_loss_amount * 1.2
        ) or (
            stats.win_rate_pct is not None
            and stats.recent_win_rate_pct is not None
            and stats.recent_win_rate_pct + 10 < stats.win_rate_pct
        ):
            recent_tone = "warning"
            recent_hypothesis = "直近では過去より執行が荒くなっている可能性があります。"
        elif (
            stats.avg_loss_amount is not None
            and stats.recent_avg_loss_amount is not None
            and stats.recent_avg_loss_amount < stats.avg_loss_amount * 0.85
        ) or (
            stats.win_rate_pct is not None
            and stats.recent_win_rate_pct is not None
            and stats.recent_win_rate_pct > stats.win_rate_pct + 10
        ):
            recent_tone = "positive"
            recent_hypothesis = "直近では過去より執行が安定してきている可能性があります。"
    cards.append(
        AnalysisDiagnosisCardRead(
            key="recent_change",
            title="最近の変化",
            hypothesis=recent_hypothesis,
            summary=recent_summary,
            evidence=[
                f"全履歴 勝率: {stats.win_rate_pct or 0:.1f}% / 直近: {stats.recent_win_rate_pct or 0:.1f}%",
                f"全履歴 平均利益額: {stats.avg_win_profit_amount or 0:.1f} / 直近: {stats.recent_avg_win_profit_amount or 0:.1f}",
                f"全履歴 平均損失額: {stats.avg_loss_amount or 0:.1f} / 直近: {stats.recent_avg_loss_amount or 0:.1f}",
            ],
            tone=recent_tone,
        )
    )

    return cards


def _top_bucket_by_loss(stats: AnalysisStatsRead) -> Optional[AnalysisHoldingBucketRead]:
    populated = [item for item in stats.holding_buckets if item.closed_trade_count > 0 and item.avg_loss_amount is not None]
    if not populated:
        return None
    return max(populated, key=lambda item: (item.avg_loss_amount or 0.0, item.closed_trade_count))


def _top_bucket_by_net_weakness(stats: AnalysisStatsRead) -> Optional[AnalysisHoldingBucketRead]:
    populated = [item for item in stats.holding_buckets if item.closed_trade_count > 0 and item.avg_net_profit_amount is not None]
    if not populated:
        return None
    return min(populated, key=lambda item: ((item.avg_net_profit_amount or 0.0), -item.closed_trade_count))


def _select_top_improvement(stats: AnalysisStatsRead) -> AnalysisTopImprovementRead:
    candidates: list[tuple[float, int, AnalysisTopImprovementRead]] = []
    currency = stats.primary_profit_currency or "JPY"
    unit = "$" if currency == "USD" else "円"

    if stats.avg_win_profit_amount is not None and stats.avg_loss_amount is not None and stats.avg_loss_amount > 0:
        ratio = (stats.profit_loss_ratio or 0.0)
        severity = max(0.0, 1.2 - ratio) + max(0.0, 0.5 - ((stats.win_rate_pct or 0.0) / 100.0))
        if severity > 0:
            candidates.append(
                (
                    severity,
                    0,
                    AnalysisTopImprovementRead(
                        key="pnl_structure",
                        title="大きな負けを先に抑える",
                        message="平均損失額が重いため、大きな負けの張り方と撤退基準を先に見直してください。",
                        rationale=[
                            f"平均利益額 {stats.avg_win_profit_amount:.1f}{unit} / 平均損失額 {stats.avg_loss_amount:.1f}{unit}",
                            f"利益額/損失額比 {stats.profit_loss_ratio or 0:.2f}x",
                            f"勝率 {stats.win_rate_pct or 0:.1f}%",
                        ],
                    ),
                )
            )

    if stats.avg_win_holding_days is not None and stats.avg_loss_holding_days is not None:
        day_gap = (stats.avg_loss_holding_days or 0.0) - (stats.avg_win_holding_days or 0.0)
        weakest_bucket = _top_bucket_by_net_weakness(stats)
        highest_loss_bucket = _top_bucket_by_loss(stats)
        severity = max(0.0, (day_gap - 2.0) / 5.0)
        if weakest_bucket and weakest_bucket.avg_net_profit_amount is not None and weakest_bucket.avg_net_profit_amount < 0:
            severity += abs(weakest_bucket.avg_net_profit_amount) / max((stats.avg_loss_amount or 1.0), 1.0)
        if severity > 0:
            bucket_label = weakest_bucket.label if weakest_bucket else "長めの保有帯"
            candidates.append(
                (
                    severity,
                    1,
                    AnalysisTopImprovementRead(
                        key="holding_execution",
                        title="保有日数を先に見直す",
                        message=f"{bucket_label} のトレードを優先して見直し、長引く負けを減らしてください。",
                        rationale=[
                            f"勝ち平均保有日数 {stats.avg_win_holding_days or 0:.1f}日 / 負け平均保有日数 {stats.avg_loss_holding_days or 0:.1f}日",
                            f"弱い保有帯: {bucket_label}",
                            (
                                f"最大平均損失帯: {highest_loss_bucket.label} / {highest_loss_bucket.avg_loss_amount or 0:.1f}{unit}"
                                if highest_loss_bucket
                                else "保有帯の差は確認中です。"
                            ),
                        ],
                    ),
                )
            )

    if stats.recent_closed_trade_count >= 5:
        recent_severity = 0.0
        if stats.avg_loss_amount is not None and stats.recent_avg_loss_amount is not None and stats.avg_loss_amount > 0:
            recent_severity += max(0.0, (stats.recent_avg_loss_amount - stats.avg_loss_amount) / stats.avg_loss_amount)
        if stats.win_rate_pct is not None and stats.recent_win_rate_pct is not None:
            recent_severity += max(0.0, ((stats.win_rate_pct - stats.recent_win_rate_pct) / 100.0))
        if recent_severity > 0.25:
            candidates.append(
                (
                    recent_severity,
                    2,
                    AnalysisTopImprovementRead(
                        key="recent_change",
                        title="最近の崩れを止める",
                        message="直近の成績悪化が見えるため、最近20件の負けトレードから先に見直してください。",
                        rationale=[
                            f"全履歴 勝率 {stats.win_rate_pct or 0:.1f}% / 直近 {stats.recent_win_rate_pct or 0:.1f}%",
                            f"全履歴 平均損失額 {stats.avg_loss_amount or 0:.1f}{unit} / 直近 {stats.recent_avg_loss_amount or 0:.1f}{unit}",
                            f"全履歴 平均利益額 {stats.avg_win_profit_amount or 0:.1f}{unit} / 直近 {stats.recent_avg_win_profit_amount or 0:.1f}{unit}",
                        ],
                    ),
                )
            )

    largest_loss_bucket = _top_bucket_by_loss(stats)
    if largest_loss_bucket and largest_loss_bucket.avg_loss_amount and stats.avg_loss_amount:
        sizing_severity = max(0.0, (largest_loss_bucket.avg_loss_amount - stats.avg_loss_amount) / stats.avg_loss_amount)
        if sizing_severity > 0.6:
            candidates.append(
                (
                    sizing_severity,
                    3,
                    AnalysisTopImprovementRead(
                        key="position_sizing",
                        title="大きく張る場面を絞る",
                        message="損失が大きい帯が目立つため、負けやすい保有パターンでは張り方を抑えてください。",
                        rationale=[
                            f"最大平均損失帯: {largest_loss_bucket.label}",
                            f"その帯の平均損失額 {largest_loss_bucket.avg_loss_amount or 0:.1f}{unit}",
                            f"全体平均損失額 {stats.avg_loss_amount or 0:.1f}{unit}",
                        ],
                    ),
                )
            )

    if not candidates:
        return AnalysisTopImprovementRead(
            key="holding_execution",
            title="詳細トレードの振り返りを進める",
            message="まずは負けトレードを2〜3件見直し、保有日数と撤退判断の癖を確認してください。",
            rationale=[
                f"決済済み {stats.closed_trade_count}件",
                f"勝率 {stats.win_rate_pct or 0:.1f}%",
                f"平均保有日数 {stats.avg_holding_days or 0:.1f}日",
            ],
        )

    _, _, best = max(candidates, key=lambda item: (item[0], -item[1]))
    return best


def _build_headline_summary(stats: AnalysisStatsRead, top_improvement: AnalysisTopImprovementRead) -> str:
    if top_improvement.key == "pnl_structure":
        if (stats.profit_loss_ratio or 0) >= 1:
            return "利幅では補えている一方で、大きな負けを抑えるとさらに安定しやすいスタイルです。"
        return "大きな負けが収支を圧迫しやすく、まず損失側の歪みを整えたいスタイルです。"
    if top_improvement.key == "holding_execution":
        return "保有を引っ張った場面で崩れやすく、保有日数のコントロールが鍵になりやすいスタイルです。"
    if top_improvement.key == "recent_change":
        return "全体の型はある一方で、直近の執行に崩れが出ていないかを先に点検したい状態です。"
    if top_improvement.key == "position_sizing":
        return "張り方の強弱が収支に影響しやすく、負けやすい場面でのサイズ管理が鍵になるスタイルです。"
    return "全体像は見え始めているので、まずは最も強い歪みから1点だけ直す段階です。"


def _build_rule_based_sections(
    stats: AnalysisStatsRead, closed: list[ClosedTradeSnapshot]
) -> tuple[
    str,
    str,
    AnalysisTopImprovementRead,
    list[AnalysisDiagnosisCardRead],
    list[str],
    list[str],
    list[str],
    list[AnalysisReviewGapRead],
]:
    wins = [item for item in closed if item.profit_value > 0]
    losses = [item for item in closed if item.profit_value < 0]
    diagnoses = _build_diagnosis_cards(stats, closed)
    review_gaps = _review_gap_reads(closed)
    top_improvement = _select_top_improvement(stats)
    headline_summary = _build_headline_summary(stats, top_improvement)
    gap_summary = " / ".join(f"{item.label} {item.missing_count}件" for item in review_gaps[:3]) or "補助情報の欠損は少なめです"
    summary = (
        f"決済済みトレードは {stats.closed_trade_count} 件です。"
        f" まずは収支構造、保有の歪み、最近の変化の3点から売買スタイルを診断します。"
        f" 補助情報では {gap_summary} が残っています。"
    )

    win_patterns: list[str] = []
    if stats.avg_win_profit_amount is not None:
        win_patterns.append(f"平均利益額は {stats.avg_win_profit_amount:.1f} です。")
    if wins:
        win_patterns.append(_holding_tendency_label(wins, losses))
    if stats.recent_win_rate_pct is not None:
        win_patterns.append(f"直近{stats.recent_closed_trade_count}件の勝率は {stats.recent_win_rate_pct:.1f}% です。")

    loss_patterns: list[str] = []
    if stats.avg_loss_amount is not None:
        loss_patterns.append(f"平均損失額は {stats.avg_loss_amount:.1f} です。")
    if stats.profit_loss_ratio is not None:
        loss_patterns.append(f"利益額/損失額比は {stats.profit_loss_ratio:.2f} です。")
    if review_gaps:
        loss_patterns.append(f"補助情報では {review_gaps[0].label} の未入力が最も多い状態です。")

    actions: list[str] = []
    if diagnoses:
        actions.append(f"まずは「{diagnoses[0].title}」の仮説を詳細トレードで確認してください。")
    if stats.holding_buckets:
        weakest = min(
            [item for item in stats.holding_buckets if item.closed_trade_count > 0],
            key=lambda item: (item.avg_net_profit_amount or 999999.0, -item.closed_trade_count),
            default=None,
        )
        if weakest:
            actions.append(f"{weakest.label} のトレードを見直し、保有ルールの仮説を1つ残してください。")
    if review_gaps:
        actions.append(f"補助情報では {review_gaps[0].label} を埋めると、次の分析精度が上がります。")

    return (
        headline_summary,
        summary,
        top_improvement,
        diagnoses[:3],
        win_patterns[:3],
        loss_patterns[:3],
        actions[:3],
        review_gaps[:5],
    )


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


def _import_review_focus(closed: list[ClosedTradeSnapshot], latest_import: Optional[AnalysisLatestImportRead]) -> list[str]:
    if latest_import is None:
        return []
    focus: list[str] = []
    changed = int(latest_import.created_count or 0) + int(latest_import.updated_count or 0)
    if changed > 0:
        focus.append(f"直近取込で追加・更新された取引が {changed} 件あります。まず大きな損失と未レビューを確認してください。")
    if latest_import.audit_gap_jpy is not None:
        gap = round(float(latest_import.audit_gap_jpy))
        if abs(gap) > 0:
            focus.append(f"実現損益CSVとの差額は {gap:+,} 円です。整合性チェックの差分銘柄を確認してください。")
        else:
            focus.append("実現損益CSVとの合計差額は 0 円です。分析へ進める状態です。")
    unreviewed = [item for item in closed if not item.review_done]
    if unreviewed:
        focus.append(f"未レビューの決済済みトレードが {len(unreviewed)} 件あります。損益の大きい順に3件だけ見直すのがおすすめです。")
    losses = sorted([item for item in closed if item.profit_value < 0], key=lambda item: item.profit_value)
    if losses:
        worst = losses[0]
        focus.append(f"{worst.symbol} の損失トレードを優先して振り返ると、次の改善点が見つかりやすいです。")
    return focus[:4]


def build_analysis_summary(
    trades: list[Trade],
    user_id: Optional[str],
    latest_import: Optional[AnalysisLatestImportRead] = None,
) -> AnalysisSummaryRead:
    stats, closed = _build_stats(trades)
    generated_at = _utc_now_iso()
    enough_data = stats.closed_trade_count >= MIN_CLOSED_TRADES_FOR_AI
    signature = _trade_signature(trades)
    cache_key = f"{user_id or 'public'}:{signature}:{_latest_import_signature(latest_import)}"
    ttl_seconds = max(30, int(settings.analysis_cache_ttl_seconds))
    now_ts = datetime.now(timezone.utc).timestamp()

    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached and cached[0] > now_ts:
            return cached[1]

    (
        rule_headline_summary,
        rule_summary,
        rule_top_improvement,
        rule_diagnoses,
        rule_win_patterns,
        rule_loss_patterns,
        rule_actions,
        rule_review_gaps,
    ) = _build_rule_based_sections(stats, closed)

    if not enough_data:
        result = AnalysisSummaryRead(
            headline_summary=rule_headline_summary,
            top_improvement=rule_top_improvement,
            summary=rule_summary,
            diagnoses=rule_diagnoses,
            win_patterns=rule_win_patterns,
            loss_patterns=rule_loss_patterns,
            actions=rule_actions,
            stats=stats,
            review_gaps=rule_review_gaps,
            latest_import=latest_import,
            import_review_focus=_import_review_focus(closed, latest_import),
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
            headline_summary=rule_headline_summary,
            top_improvement=rule_top_improvement,
            summary=summary,
            diagnoses=rule_diagnoses,
            win_patterns=win_patterns,
            loss_patterns=loss_patterns,
            actions=actions,
            stats=stats,
            review_gaps=rule_review_gaps,
            latest_import=latest_import,
            import_review_focus=_import_review_focus(closed, latest_import),
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
            headline_summary=rule_headline_summary,
            top_improvement=rule_top_improvement,
            summary=rule_summary,
            diagnoses=rule_diagnoses,
            win_patterns=rule_win_patterns,
            loss_patterns=rule_loss_patterns,
            actions=rule_actions,
            stats=stats,
            review_gaps=rule_review_gaps,
            latest_import=latest_import,
            import_review_focus=_import_review_focus(closed, latest_import),
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
                headline_summary=rule_headline_summary,
                top_improvement=rule_top_improvement,
                summary=summary,
                diagnoses=rule_diagnoses,
                win_patterns=win_patterns,
                loss_patterns=loss_patterns,
                actions=actions,
                stats=stats,
                review_gaps=rule_review_gaps,
                latest_import=latest_import,
                import_review_focus=_import_review_focus(closed, latest_import),
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
                headline_summary=rule_headline_summary,
                top_improvement=rule_top_improvement,
                summary=rule_summary,
                diagnoses=rule_diagnoses,
                win_patterns=rule_win_patterns,
                loss_patterns=rule_loss_patterns,
                actions=rule_actions,
                stats=stats,
                review_gaps=rule_review_gaps,
                latest_import=latest_import,
                import_review_focus=_import_review_focus(closed, latest_import),
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
