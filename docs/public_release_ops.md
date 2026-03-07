# TradeTrace 公開運用 Runbook (v1)

このドキュメントは、公開前後の運用手順を固定するための実務手順書です。

## 1. 環境変数

### 1.1 Backend (Render)
- `DATABASE_URL`:
  - Render Postgres 接続文字列
- `AUTH_ENABLED=true`
- `SUPABASE_URL`:
  - 例: `https://<project-ref>.supabase.co`
- `SUPABASE_JWT_SECRET`:
  - Supabase Project Settings の JWT secret
- `SUPABASE_SERVICE_ROLE_KEY`:
  - Supabase service role key
- `INVITE_CODE_REQUIRED=true`
- `CORS_ALLOW_ORIGINS`:
  - 例: `https://investment-log-frontend.vercel.app,http://localhost:5173`
- `RATE_LIMIT_ENABLED=true`
- `RATE_LIMIT_PER_MINUTE=120`
- `OPS_ALERT_TARGET`:
  - 例: `slack:#tradetrace-alerts` / `email:ops@example.com`
- `DB_BACKUP_STRATEGY`:
  - 例: `render-managed-daily`

### 1.2 Frontend (Vercel)
- `VITE_API_BASE`:
  - 例: `https://<render-backend-host>/api/v1`
- `VITE_API_TIMEOUT_MS` (任意):
  - 例: `15000`
- `VITE_AUTH_ENABLED=true`
- `VITE_SUPABASE_URL`:
  - 例: `https://<project-ref>.supabase.co`
- `VITE_SUPABASE_ANON_KEY`:
  - Supabase anon key

## 2. Supabase Auth 設定

### 2.1 Site URL
- `https://investment-log-frontend.vercel.app`

### 2.2 Redirect URL
- 本番:
  - `https://investment-log-frontend.vercel.app/auth/callback`
- ローカル:
  - `http://localhost:5173/auth/callback`

### 2.3 メール認証
- Magic Link を有効化
- テンプレートに不審な文言がないか確認

## 3. DBマイグレーション

公開前に backend で必ず実行:

```bash
cd backend
alembic upgrade head
```

## 3.1 設定チェック
公開前に不足設定を確認:

```bash
cd backend
.venv/bin/python tools/check_release_config.py
```

`AUTH_ENABLED=true` の場合は、上記でDBスキーマも同時に検証する
（`trades.user_id`, `invite_codes`, `invite_codes.used_at`）。

本番反映直前は strict モード推奨:
```bash
cd backend
.venv/bin/python tools/check_release_config.py --strict
```

`AUTH_ENABLED=true` で `RATE_LIMIT_ENABLED=false` の場合は warning が出る。

## 4. 招待コード運用

### 4.1 新規発行
```bash
cd backend
.venv/bin/python tools/create_invite_code.py --days 7 --length 10
```

出力例:
- `invite_code=AB12CD34EF`
- `expires_at=2026-03-14T...`

### 4.2 固定コードで発行 (必要時のみ)
```bash
cd backend
.venv/bin/python tools/create_invite_code.py --code AB12CD34EF --days 7
```

### 4.3 ルール
- 1コード1人
- 7日で失効
- SNS等に公開しない

### 4.4 一覧確認
```bash
cd backend
.venv/bin/python tools/manage_invite_codes.py list --status all --limit 50
```

### 4.5 無効化
コード文字列で無効化:
```bash
cd backend
.venv/bin/python tools/manage_invite_codes.py revoke --code AB12CD34EF
```

ID指定で無効化:
```bash
cd backend
.venv/bin/python tools/manage_invite_codes.py revoke --id 12
```

## 5. リリース手順 (推奨順)
1. backend の env を更新
2. frontend の env を更新
3. `alembic upgrade head` を実行
4. `tools/check_release_config.py` を実行
5. 招待コードを1件発行
6. テストユーザーで以下を確認
   - ログイン画面表示
   - 招待コードありでログイン成功
   - `Trades` 作成/編集/削除
   - `Settings` でエクスポート可能
   - `GET /health/ready` が `200` を返す
7. 招待ユーザーへ URL とコードを配布

## 6. スモークチェック

一括実行:
```bash
cd backend
.venv/bin/python tools/smoke_release.py --base https://<render-backend-host>
```

### 6.1 認証
- 未ログインで `/trades` へ行くと `/auth` へ遷移する
- 無効コードでは API が 403 を返す
- `GET /health/ready` が `{"status":"ok","db":"ok"}` を返す
- レスポンスヘッダ `X-Request-ID` が付与される

### 6.2 データ分離
- Aユーザーで作成したトレードが Bユーザーで見えない

### 6.3 Settings
- `Account` 情報が取得できる
- JSON/CSV エクスポートがダウンロードできる
- データ削除で対象ユーザーの trade が削除される

## 7. 既知の注意点
- `AUTH_ENABLED=false` では従来モード（ローカル開発向け）
- `SUPABASE_SERVICE_ROLE_KEY` 未設定時:
  - アプリデータ削除は実行される
  - Supabase Authユーザー削除はスキップされる
- 障害調査時は画面のエラーメッセージに出る `request_id` をログ検索キーとして使う

## 8. ロールバック方針
- 問題発生時は `AUTH_ENABLED=false` に戻して一時的に従来モードで運用継続
- 原因修正後に再度 `AUTH_ENABLED=true` へ戻す
