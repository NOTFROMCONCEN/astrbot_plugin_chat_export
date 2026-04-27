# astrbot_plugin_chat_export

基础功能：按群监听聊天，支持群历史补录，导出 TXT，并支持 Qdrant 语义检索。

## 指令

- `/聊天监听 <开始|停止|状态> [群号]`
- `/聊天历史同步 [条数] [群号]`
- `/聊天导出 <开始时间> <结束时间> [群号]`
- `/聊天检索 [群号] [最近1小时|recent:2h] <问题>`
- `/聊天统计 [群号]`
- `/聊天健康`

英文兼容：

- `/chat_listen ...`
- `/chat_history_sync ...`
- `/chat_export ...`
- `/chat_search ...`
- `/chat_stats ...`
- `/chat_health`

说明：历史同步目前仅支持 aiocqhttp 平台下的 OneBot V11 协议端，例如 NapCat、Lagrange。

## 本次优化

- 历史补录：支持通过 OneBot 历史 API 拉取群聊历史，并按消息时间写入 SQLite
- 历史同步链路优化：自动缓存成功的 OneBot 历史接口策略，减少后续分页时的无效重试
- SQLite 去重：`unique_key` 唯一索引，避免重复消息入库
- SQLite 性能：WAL + NORMAL + busy_timeout
- Qdrant 批量写：队列 + 批量 embedding + 批量 upsert
- 检索时间过滤：支持 `最近1小时` / `recent:2h`，并支持默认回溯窗口
- 多媒体索引策略：`index_media_placeholders=false` 时不索引纯占位内容
- 健康监控：`/聊天健康` 输出吞吐、最近成功时间、最近错误、队列积压

## 推荐配置

- `listening_group_ids`: 需要监听的群号
- `history_sync_default_limit=100`: 历史同步默认补录条数
- `history_sync_max_limit=1000`: 历史同步单次最大条数
- `history_sync_page_size=20`: 历史同步分页大小
- `history_sync_action_timeout_sec=8.0`: 单次历史 API 请求超时，避免协议端卡死时用户侧无响应
- `stop_event_after_ingest=true`: 先采集再静默（只记录不回复）
- `qdrant_enabled=true`
- `search_default_since_hours=24`
- `qdrant_batch_size=20`
- `qdrant_flush_interval_sec=1.0`

## 与强制静默协同

推荐 `force_silent.cooperative_mode=true`，避免其提前 `stop_event` 导致采集插件收不到消息。
