# P1 polish: 楽天CSV形式不一致エラーの内部カラム名をユーザー向け文言にする

## 背景
公開v1 Production再E2Eで、形式不一致CSVは安全に拒否されることを確認済み。

一方で、エラー本文に `date, name, price, qty, side, symbol` のような内部正規化カラム名が表示されており、初期ユーザーには少し突き放した印象になる。

## 期待する改善
- 楽天CSV以外、または列不足のCSVをアップロードした場合、内部カラム名ではなくユーザー向け文言で表示する
- 例: `楽天証券の国内株CSVとして読み取れませんでした。楽天証券の取引履歴（国内株式）CSVを再ダウンロードして、もう一度アップロードしてください。`
- 必要に応じて不足項目は日本語ラベルで補足する
- analytics の `csv_preview_failure` / `error_type=csv_missing_headers` は維持する

## 受け入れ条件
- 形式不一致CSV preview で `missing_headers` 相当が発生しても、UIに内部キー名がそのまま出ない
- エラー理由と次にやることが日本語で分かる
- 既存の安全拒否、error_count 表示、KPIイベント発火は維持される
- 楽天CSV正常系と同一CSV再取込の挙動に影響しない

## 証跡
- Production再E2E結果:
  - `/Users/hiroki/Projects/TradeTrace/frontend/docs/release_e2e_runs/2026-05-05_production_rakuten_csv_v1_resume_result.json`
- エラー画面スクリーンショット:
  - `/Users/hiroki/Projects/TradeTrace/frontend/docs/release_e2e_runs/screenshots_2026-05-05-production/18_resume_preview_invalid_format.png`

## 優先度
P1 polish。

限定公開Goのブロッカーではないが、初期ユーザー投入前後で早めに改善したい。

## GitHub issue作成メモ
2026-05-05時点では、Codex GitHubコネクタで `Okapiron/investment-log-backend` への issue 作成を試みたが、GitHub API が `Resource not accessible by integration` を返した。

`gh` CLI もローカル環境に未導入だったため、正式なGitHub issue化はリポジトリ権限またはCLI準備後に実施する。
