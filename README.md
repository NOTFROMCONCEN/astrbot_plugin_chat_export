# astrbot_plugin_chat_export

基礎功能：按群監聽聊天、補錄群歷史、匯出 TXT，並支援 Qdrant 語義檢索與聊天分析。

## 指令

- `/聊天監聽 <開始|停止|狀態> [群號]`
- `/聊天歷史同步 [條數] [群號]`
- `/聊天匯出 <開始時間> <結束時間> [群號]`
- `/聊天檢索 [群號] [最近1小時|recent:2h] <問題>`
- `/聊天分析 [群號] [使用者ID] [最近24小時|recent:2d]`
- `/聊天統計 [群號]`
- `/聊天健康`

英文相容：

- `/chat_listen ...`
- `/chat_history_sync ...`
- `/chat_export ...`
- `/chat_search ...`
- `/chat_analyze ...`
- `/chat_stats ...`
- `/chat_health`

說明：歷史同步目前僅支援 aiocqhttp 平台下的 OneBot V11 協議端，例如 NapCat、Lagrange。

## 2.0.6 優化

- README 全面轉為繁體中文，並補齊聊天分析指令與設定說明
- 聊天分析：可按群、使用者與時間視窗抽樣聊天記錄，呼叫 OpenAI 相容模型產出摘要
- 聊天分析降級：模型拒答或未取得結果時，可回退本地統計摘要
- 歷史補錄：透過 OneBot 歷史 API 拉取群聊歷史，按訊息時間寫入 SQLite
- 歷史同步鏈路：自動快取成功的 OneBot 歷史介面策略，減少後續分頁時的無效重試
- SQLite 去重：使用 `unique_key` 唯一索引，避免重複訊息入庫
- Qdrant 批次寫入：佇列、批次 embedding、批次 upsert
- 檢索時間過濾：支援 `最近1小時` / `recent:2h`，並支援預設回溯視窗
- 多媒體索引策略：`index_media_placeholders=false` 時不索引純占位內容
- 健康監控：`/聊天健康` 輸出吞吐、最近成功時間、最近錯誤與佇列積壓

## 推薦設定

- `listening_group_ids`: 需要監聽的群號
- `history_sync_default_limit=100`: 歷史同步預設補錄條數
- `history_sync_max_limit=1000`: 歷史同步單次最大條數
- `history_sync_page_size=20`: 歷史同步分頁大小
- `history_sync_action_timeout_sec=8.0`: 單次歷史 API 請求逾時，避免協議端卡住時使用者側無回應
- `stop_event_after_ingest=true`: 先採集再靜默，只記錄不回覆
- `qdrant_enabled=true`
- `search_default_since_hours=24`
- `qdrant_batch_size=20`
- `qdrant_flush_interval_sec=1.0`
- `analysis_api_base`: 聊天分析模型 Base URL，留空時沿用 embedding 設定
- `analysis_api_key`: 聊天分析模型 Key，留空時回退 `embedding_api_key`
- `analysis_model=gpt-4o-mini`
- `analysis_default_since_hours=72`

## 與強制靜默協同

推薦 `force_silent.cooperative_mode=true`，避免其提前 `stop_event` 導致採集插件收不到訊息。
