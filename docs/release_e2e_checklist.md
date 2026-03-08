# TradeTrace 公開 E2Eチェックリスト (v2)

このチェックリストは、公開前に「実利用の一連フロー」が壊れていないことを確認するための手順です。
対象は Phase 1（メール+パスワード認証 / Invite OFF）です。

## 0. 使い方
- 実施日、実施者、対象環境（Production / Staging）を最初に記録する
- 各項目を `PASS / FAIL / SKIP` で記録する
- `FAIL` が1件でもあれば公開を止める

## 1. 事前準備
- [ ] Backend の release preflight が通る
  - `cd backend && .venv/bin/python tools/preflight_release.py --base https://<render-backend-host>`
- [ ] Frontend の release preflight が通る
  - `cd frontend && npm run preflight:release`
- [ ] Runtime で `Auth: ON / Invite: OFF` を確認できる

## 2. ケースA: 新規ユーザー登録とログイン導線
- [ ] `https://investment-log-frontend.vercel.app/auth` が表示される
- [ ] 新規登録（メール+パスワード）で登録できる
- [ ] 登録済みメールで新規登録した場合、`User already registered` になる
- [ ] ログイン（メール+パスワード）で `/trades` に遷移できる
- [ ] `/trades/new` から新規トレードを保存できる
- [ ] `/trades/:id` で編集・保存できる
- [ ] レビュー完了条件を満たしたときのみ「レビュー済」にできる
- [ ] 条件を崩した編集をすると「未レビュー」へ戻る

## 3. ケースB: 認証エラー導線
- [ ] 誤ったパスワードでログインした場合、`Invalid login credentials` になる
- [ ] 未ログインで `/trades` にアクセスした場合、`/auth` へ遷移する
- [ ] 既存ユーザーの再ログインが問題なくできる

## 4. ケースC: ユーザー分離
- [ ] ユーザーAで作成したトレードがユーザーBには見えない
- [ ] ユーザーBからユーザーAのトレード詳細URLへ直接アクセスしても取得できない
- [ ] 一覧件数・集計が各ユーザーで独立している

## 5. ケースD: Settings 機能
- [ ] `Settings > Account` で user_id / email が取得できる
- [ ] `Settings > Runtime` が表示される（Version / Release Status）
- [ ] JSONエクスポートをダウンロードできる
- [ ] CSVエクスポートをダウンロードできる
- [ ] `DELETE` 入力なしではデータ削除できない
- [ ] `DELETE` 入力ありではデータ削除でき、再ログイン導線が壊れない

## 6. ケースE: API生存
- [ ] `GET https://<render-backend-host>/health` が `200`
- [ ] `GET https://<render-backend-host>/health/ready` が `200`
- [ ] `GET https://<render-backend-host>/openapi.json` が `200`

## 7. 公開可否判定
- [ ] FAIL が 0 件
- [ ] 重大な WARNING が残っていない（公開判断に影響しないと確認済み）
- [ ] 判定: `GO / NO-GO`

## 8. 実施記録テンプレ
- 実施日:
- 実施者:
- 対象環境:
- Backend Version:
- Frontend Version:
- 結果サマリ:
- FAIL項目と対応:
- 最終判定:
