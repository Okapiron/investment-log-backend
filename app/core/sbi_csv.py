from __future__ import annotations

import csv
from decimal import Decimal
import hashlib
import io
from typing import Optional

from app.core.rakuten_csv import (
    _AggregatedTrade,
    _RawCsvTrade,
    _aggregate_rows,
    _clean_text,
    _normalize_header,
    _pair_round_trips,
    _parse_date,
    _parse_jp_decimal,
    _parse_jp_int,
)
from app.schemas.imports import (
    ImportIssueRead,
    RakutenAuditRowRead,
    RakutenImportAuditResponse,
    RakutenImportPreviewResponse,
)

_HEADER_ALIASES = {
    "date": {"約定日", "取引日", "受渡日"},
    "symbol": {"銘柄コード", "コード", "銘柄ｺｰﾄﾞ"},
    "name": {"銘柄", "銘柄名", "銘柄名称"},
    "side": ("売買", "売買区分", "取引"),
    "qty": {"数量", "株数", "約定数量", "数量［株］", "数量[株]"},
    "price": {"約定単価", "単価", "売買単価", "約定価格", "単価［円］", "単価[円]"},
    "fee": {"手数料", "委託手数料", "手数料［円］", "手数料[円]"},
    "tax_fee": {"税金等", "消費税", "手数料消費税", "税金等［円］", "税金等[円]"},
    "other_fee": {"諸費用", "金利", "貸株料", "逆日歩", "管理費", "諸費用［円］", "諸費用[円]"},
    "trade_type": ("取引区分", "取引種別", "商品", "現物信用", "預り区分", "取引"),
    "credit_type": ("信用区分", "新規返済", "新規/返済", "建区分"),
    "build_date": {"建約定日", "建日", "建日付"},
    "build_price": {"建単価", "建単価［円］", "建単価[円]", "平均取得価額"},
    "build_fee": {"建手数料", "建手数料［円］", "建手数料[円]"},
    "build_fee_tax": {"建手数料消費税", "建手数料消費税［円］", "建手数料消費税[円]"},
    "settlement_amount": {"受渡金額", "受渡金額［円］", "受渡金額[円]", "精算金額"},
    "realized_profit": {"実現損益", "損益", "譲渡損益", "実現損益［円］", "実現損益[円]"},
    "avg_cost": {"平均取得価額", "取得単価", "買付単価"},
    "sell_price": {"売却/決済単価", "売却単価", "決済単価", "売単価", "売買単価"},
    "sell_date": {"約定日", "決済日", "売却日", "受渡日"},
}


def _headers(fieldnames: list[str] | None) -> dict[str, str]:
    normalized = {_normalize_header(name): name for name in (fieldnames or [])}
    found: dict[str, str] = {}
    for key, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            name = normalized.get(_normalize_header(alias))
            if name:
                found[key] = name
                break
    return found


def _value(row: dict[str, str], headers: dict[str, str], key: str) -> str:
    name = headers.get(key)
    return _clean_text(row.get(name, "")) if name else ""


def _trade_kind(row: dict[str, str], headers: dict[str, str]) -> str:
    value = f"{_value(row, headers, 'trade_type')} {_value(row, headers, 'credit_type')}"
    return "credit" if "信用" in value else "spot"


def _is_credit_close(row: dict[str, str], headers: dict[str, str]) -> bool:
    value = f"{_value(row, headers, 'side')} {_value(row, headers, 'trade_type')} {_value(row, headers, 'credit_type')}"
    return "信用" in value and ("返済" in value or "現引" in value or "現渡" in value)


def _side(row: dict[str, str], headers: dict[str, str]) -> Optional[str]:
    value = f"{_value(row, headers, 'side')} {_value(row, headers, 'trade_type')} {_value(row, headers, 'credit_type')}"
    if "売" in value:
        return "sell"
    if "買" in value:
        return "buy"
    return None


def _position_side(row: dict[str, str], headers: dict[str, str]) -> str:
    side = _value(row, headers, "side")
    trade_type = _value(row, headers, "trade_type")
    credit_type = _value(row, headers, "credit_type")
    value = f"{side} {trade_type} {credit_type}"
    if "信用" in value:
        if ("新規" in value and "売" in side) or "売建" in value or "新規売" in value or "売新規" in value:
            return "short"
        if ("返済" in value and "買" in side) or "買埋" in value or "返済買" in value or "買返済" in value:
            return "short"
    return "long"


def _required_missing(headers: dict[str, str]) -> list[str]:
    missing = []
    for key in ("date", "symbol", "side", "qty", "price"):
        if key not in headers:
            missing.append(key)
    return missing


def _as_sbi_candidate_signatures(preview: RakutenImportPreviewResponse) -> RakutenImportPreviewResponse:
    for item in preview.candidates:
        old_position = item.source_position_key
        item.source_position_key = hashlib.sha1(f"sbi|{old_position}".encode("utf-8")).hexdigest()
        item.source_signature = hashlib.sha1(f"sbi|{item.source_signature}".encode("utf-8")).hexdigest()
    preview.broker = "sbi"
    return preview


def parse_sbi_domestic_csv(content: str, filename: Optional[str] = None) -> RakutenImportPreviewResponse:
    stream = io.StringIO(content)
    reader = csv.DictReader(stream)
    headers = _headers(reader.fieldnames)
    missing = _required_missing(headers)
    if missing:
        return RakutenImportPreviewResponse(
            broker="sbi",
            market_scope="JP",
            filename=filename,
            candidate_count=0,
            skipped_count=0,
            error_count=1,
            candidates=[],
            skipped=[],
            errors=[ImportIssueRead(line=None, code="missing_headers", message=f"CSVヘッダーが不足しています: {', '.join(missing)}")],
        )

    raw_rows: list[_RawCsvTrade] = []
    skipped: list[ImportIssueRead] = []
    errors: list[ImportIssueRead] = []
    for line, row in enumerate(reader, start=2):
        symbol = _value(row, headers, "symbol")
        date = _parse_date(_value(row, headers, "date"))
        qty = _parse_jp_int(_value(row, headers, "qty"))
        price = _parse_jp_decimal(_value(row, headers, "price"))
        side = _side(row, headers)
        if not symbol or not date or not qty or not price or not side:
            skipped.append(ImportIssueRead(line=line, code="unsupported_row", message="国内株の約定行として読み取れませんでした。"))
            continue
        fee_commission = _parse_jp_int(_value(row, headers, "fee")) or 0
        fee_tax = _parse_jp_int(_value(row, headers, "tax_fee")) or 0
        fee_other = _parse_jp_int(_value(row, headers, "other_fee")) or 0
        build_fee = _parse_jp_int(_value(row, headers, "build_fee")) or 0
        build_fee_tax = _parse_jp_int(_value(row, headers, "build_fee_tax")) or 0
        raw_rows.append(
            _RawCsvTrade(
                line=line,
                symbol=symbol,
                name=_value(row, headers, "name") or symbol,
                trade_kind=_trade_kind(row, headers),
                side=side,
                position_side=_position_side(row, headers),
                date=date,
                qty=qty,
                price=price,
                fee=fee_commission + fee_tax + fee_other,
                fee_commission_jpy=fee_commission,
                fee_tax_jpy=fee_tax,
                fee_other_jpy=fee_other,
                build_date=_parse_date(_value(row, headers, "build_date")),
                build_price=_parse_jp_decimal(_value(row, headers, "build_price")),
                build_fee=build_fee + build_fee_tax,
                build_fee_commission_jpy=build_fee,
                build_fee_tax_jpy=build_fee_tax,
                is_credit_close=_is_credit_close(row, headers),
                settlement_amount_jpy=_parse_jp_int(_value(row, headers, "settlement_amount")),
            )
        )

    candidates, pair_skipped, pair_errors = _pair_round_trips(_aggregate_rows(raw_rows))
    preview = RakutenImportPreviewResponse(
        broker="sbi",
        market_scope="JP",
        filename=filename,
        candidate_count=len(candidates),
        skipped_count=len(skipped) + len(pair_skipped),
        error_count=len(errors) + len(pair_errors),
        candidates=candidates,
        skipped=[*skipped, *pair_skipped],
        errors=[*errors, *pair_errors],
    )
    return _as_sbi_candidate_signatures(preview)


def _realized_rows(content: str) -> list[RakutenAuditRowRead]:
    reader = csv.DictReader(io.StringIO(content))
    headers = _headers(reader.fieldnames)
    rows: list[RakutenAuditRowRead] = []
    for row in reader:
        symbol = _value(row, headers, "symbol")
        sell_date = _parse_date(_value(row, headers, "sell_date")) or _parse_date(_value(row, headers, "date"))
        qty = _parse_jp_int(_value(row, headers, "qty"))
        sell_price = _parse_jp_decimal(_value(row, headers, "sell_price") or _value(row, headers, "price"))
        avg_cost = _parse_jp_decimal(_value(row, headers, "avg_cost") or _value(row, headers, "build_price"))
        realized = _parse_jp_int(_value(row, headers, "realized_profit"))
        if not symbol or not sell_date or not qty or sell_price is None or avg_cost is None or realized is None:
            continue
        rows.append(
            RakutenAuditRowRead(
                symbol=symbol,
                name=_value(row, headers, "name") or symbol,
                sell_date=sell_date,
                qty=qty,
                sell_price=float(sell_price),
                buy_price_or_avg_cost=float(avg_cost),
                rakuten_profit_jpy=float(realized),
            )
        )
    return rows


def _audit_key(row: RakutenAuditRowRead) -> tuple[str, str, int, str]:
    return (row.symbol, row.sell_date, row.qty, f"{row.sell_price:.2f}")


def _candidate_profit(item) -> Optional[RakutenAuditRowRead]:
    open_fill = item.buy if item.position_side == "long" else item.sell
    close_fill = item.sell if item.position_side == "long" else item.buy
    if open_fill is None or close_fill is None:
        return None
    qty = Decimal(str(open_fill.qty))
    open_price = Decimal(str(open_fill.price))
    close_price = Decimal(str(close_fill.price))
    open_fee = Decimal(str(open_fill.fee_total_jpy if open_fill.fee_total_jpy is not None else open_fill.fee or 0))
    close_fee = Decimal(str(close_fill.fee_total_jpy if close_fill.fee_total_jpy is not None else close_fill.fee or 0))
    gross = (open_price - close_price) * qty if item.position_side == "short" else (close_price - open_price) * qty
    profit = gross - open_fee - close_fee
    return RakutenAuditRowRead(
        symbol=item.symbol,
        name=item.name,
        sell_date=close_fill.date,
        qty=int(close_fill.qty),
        sell_price=float(close_fill.price),
        buy_price_or_avg_cost=float(open_fill.price),
        tt_profit_jpy=float(profit),
    )


def audit_sbi_tradehistory_against_realized(
    tradehistory_content: str,
    *,
    tradehistory_filename: Optional[str] = None,
    realized_content: str,
) -> RakutenImportAuditResponse:
    preview = parse_sbi_domestic_csv(tradehistory_content, tradehistory_filename)
    tt_rows = [row for item in preview.candidates if (row := _candidate_profit(item)) is not None]
    realized_rows = _realized_rows(realized_content)
    realized_by_key: dict[tuple[str, str, int, str], list[RakutenAuditRowRead]] = {}
    for row in realized_rows:
        realized_by_key.setdefault(_audit_key(row), []).append(row)

    matched = 0
    pnl_mismatch: list[RakutenAuditRowRead] = []
    unmatched_tt: list[RakutenAuditRowRead] = []
    used_realized: set[int] = set()
    for row in tt_rows:
        bucket = realized_by_key.get(_audit_key(row), [])
        realized = next((candidate for candidate in bucket if id(candidate) not in used_realized), None)
        if realized is None:
            row.reason_code = "missing_in_realized_pl"
            unmatched_tt.append(row)
            continue
        used_realized.add(id(realized))
        row.rakuten_profit_jpy = realized.rakuten_profit_jpy
        if round(float(row.tt_profit_jpy or 0), 2) == round(float(realized.rakuten_profit_jpy or 0), 2):
            matched += 1
        else:
            row.reason_code = "cost_breakdown_mismatch"
            row.message = "SBI実現損益とTT再構成損益が一致しません。"
            pnl_mismatch.append(row)

    missing = []
    used_ids = used_realized
    for row in realized_rows:
        if id(row) not in used_ids:
            row.reason_code = "missing_in_tradehistory"
            missing.append(row)

    tt_total = sum(Decimal(str(row.tt_profit_jpy or 0)) for row in tt_rows)
    realized_total = sum(Decimal(str(row.rakuten_profit_jpy or 0)) for row in realized_rows)
    symbol_totals: dict[str, dict[str, object]] = {}
    for row in tt_rows:
        entry = symbol_totals.setdefault(row.symbol, {"symbol": row.symbol, "name": row.name, "tt": Decimal("0"), "realized": Decimal("0")})
        if row.tt_profit_jpy is not None:
            entry["tt"] = Decimal(str(entry["tt"])) + Decimal(str(row.tt_profit_jpy))
    for row in realized_rows:
        entry = symbol_totals.setdefault(row.symbol, {"symbol": row.symbol, "name": row.name, "tt": Decimal("0"), "realized": Decimal("0")})
        if row.rakuten_profit_jpy is not None:
            entry["realized"] = Decimal(str(entry["realized"])) + Decimal(str(row.rakuten_profit_jpy))
    top = []
    for entry in symbol_totals.values():
        gap = Decimal(str(entry["tt"])) - Decimal(str(entry["realized"]))
        if gap:
            top.append({"symbol": entry["symbol"], "name": entry["name"], "tt_profit_jpy": float(entry["tt"]), "rakuten_profit_jpy": float(entry["realized"]), "gap_jpy": float(gap), "reason_codes": []})
    top.sort(key=lambda item: abs(item["gap_jpy"]), reverse=True)

    return RakutenImportAuditResponse(
        preview_candidate_count=preview.candidate_count,
        tt_reconstructed_count=len(tt_rows),
        rakuten_row_count=len(realized_rows),
        tt_total_jpy=float(tt_total),
        rakuten_total_jpy=float(realized_total),
        gap_jpy=float(tt_total - realized_total),
        matched_count=matched,
        missing_in_tt=missing,
        pnl_mismatch=pnl_mismatch,
        unmatched_tt=unmatched_tt,
        top_symbol_diffs=top[:10],
        reimport_hint="SBI約定履歴CSVを主データ、SBI実現損益CSVを監査元として照合しています。",
    )
