# TradeTrace 障害時一次対応フロー (v1)

このドキュメントは、障害発生から30分以内の一次対応を標準化するための手順書です。

## 0. 対象
- API応答エラー（5xx / 429増加）
- ログイン不能
- DB接続エラー
- デプロイ後の機能不全

## 1. まず最初にやること（3分以内）
1. 影響範囲を確認:
   - 全ユーザー影響か、一部ユーザーのみか
   - どの機能で発生しているか（Auth / Trades / Settings / Export）
2. 障害メモを作成:
   - 発生時刻（JST/UTC）
   - 最初の検知経路（ユーザー報告 / 監視 / 自分で発見）
   - 現時点の症状
3. 以降の調査キーを固定:
   - 画面エラーの `request_id`
   - Runtime の `Backend Version` / `Frontend Version` / `Server Time (UTC)`

## 2. 切り分け手順（10分以内）
1. Frontend 生存確認:
   - `https://investment-log-frontend.vercel.app` が表示されるか
2. Backend 生存確認:
   - `GET https://<render-backend-host>/health`
   - `GET https://<render-backend-host>/health/ready`
3. API仕様確認:
   - `GET https://<render-backend-host>/openapi.json`
4. Settings Runtime 確認:
   - `Release Status`
   - `Invite Readiness`
   - `config_errors` / `config_warnings` の表示

## 3. 代表的な症状と初動
### 3.1 `/health` はOK、`/health/ready` が503
- DB障害の可能性が高い
- Render Postgres 側の状態を確認
- 復旧まで書き込み操作を控える案内を検討

### 3.2 認証関連で401/403が急増
- `AUTH_ENABLED`, `SUPABASE_URL`, `SUPABASE_JWT_SECRET` を確認
- Supabase 側の稼働状況を確認
- 招待制の場合は `Invite Readiness` が `READY` か確認

### 3.3 一部画面で404（直アクセス）
- 最新 frontend デプロイの反映状況を確認
- Vercel の最新 Production Deployment を確認

### 3.4 429が多発
- `RATE_LIMIT_ENABLED`, `RATE_LIMIT_PER_MINUTE` を確認
- 直近のアクセス集中有無を確認

## 4. 応急措置（必要時）
1. 重大障害時の暫定運用:
   - `AUTH_ENABLED=false` に戻して暫定継続（Runbook ロールバック方針）
2. 反映:
   - Render env 更新後、再デプロイ
3. 確認:
   - `health` / `health/ready` / 最低限の画面操作

## 5. ユーザー向け一次連絡テンプレ
「現在、TradeTrace で障害を確認しています。原因を調査中です。復旧見込みが分かり次第、改めてお知らせします。」

## 6. 復旧判定
- `health` / `health/ready` が正常
- 主要導線（ログイン、一覧、新規作成、詳細編集、エクスポート）が再現テストで成功
- Runtime の `Release Status` が想定どおり

## 7. 事後対応（24時間以内）
1. 原因・影響・対応・再発防止を簡潔に記録
2. 必要なら Runbook / CI / Runtime 診断項目を更新
3. 同種障害に備えて検知ルールを追加
