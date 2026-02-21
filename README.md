# AI Signal Board

一个可直接同步到 GitHub 的 AI 新闻聚合项目：

- 页面：高质量静态展示（`index.html`）
- 抓取：`scripts/update_news.py`
- 数据：`data/latest-24h.json` + `data/archive.json` + `data/source-status.json` + `data/waytoagi-7d.json`
- 自动更新：GitHub Actions 每 30 分钟采集一次

## 支持站点

- https://techurls.com/
- https://www.buzzing.cc/
- https://iris.findtruman.io/web/info_flow
- https://www.bestblogs.dev/en/newsletter
- https://tophub.today/
- https://zeli.app/zh
- https://ai.hubtoday.app/
- https://www.aibase.com/zh/news
- https://aihot.today/
- https://newsnow.busiyi.world/

## 设计目标（尽量不漏消息）

- 多源策略：优先官方 API/Feed，失败时回退页面结构抓取
- 高频采集：建议每 30 分钟运行一次
- 归档去重：按 `site + source + title + normalized_url` 生成稳定 ID
- 增量追踪：记录 `first_seen_at / last_seen_at / published_at`
- 24h 视图：先做全量采集，再过滤到 AI/科技/机器人/具身智能强相关主题
- 站点统计：同时展示过滤后数量和全量数量（`count/raw_count`）
- 前端双视图：`AI 强相关` / `全量` 一键切换
- 站点内聚合：选定某站点后按分区（`source`）分组展示，避免同分区被打散
- 全量视图：支持“去重开/关”开关；开启时按“原文标题 + 链接”做随机去重（仅全量生效）
- 双语显示：英文原生标题优先补中文译文（缓存到 `data/title-zh-cache.json`）
- 可选 RSS：支持从 OPML 批量导入 RSS，并输出失败源/零更新源告警

## 站点规则

- `zeli.app`：仅采集 `黑客新闻 -> 24h 最热`
- `iris / techurls / buzzing / newsnow / tophub`：尽量保留分区来源字段（`source`）

## WaytoAGI 近 7 日更新

- 入口页：`https://waytoagi.feishu.cn/wiki/QPe5w5g7UisbEkkow8XcDmOpn8e?fromScene=spaceOverview`
- 自动定位“历史更新”页并提取近 7 日条目
- 输出文件：`data/waytoagi-7d.json`

## 本地运行

```bash
cd /Users/carl/Downloads/ai-news-radar
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/update_news.py --output-dir data --window-hours 24
# 叠加 OPML RSS（例如 Follow 导出的 follow.opml）
python scripts/update_news.py --output-dir data --window-hours 24 --rss-opml /Users/carl/Downloads/follow.opml
```

启动本地预览：

```bash
python -m http.server 8080
# 打开 http://localhost:8080
```

## GitHub 自动更新

工作流文件：

- `.github/workflows/update-news.yml`

默认每 30 分钟执行一次，自动更新：

- `data/latest-24h.json`
- `data/archive.json`
- `data/source-status.json`
- `data/waytoagi-7d.json`

并自动提交到仓库。

## 目录结构

```text
ai-news-radar/
  assets/
    app.js
    styles.css
  data/
    latest-24h.json
    archive.json
    source-status.json
    waytoagi-7d.json
  scripts/
    update_news.py
  tests/
  .github/workflows/update-news.yml
  index.html
  requirements.txt
```

## 注意

- 某些站点会有反爬/CDN 限制，脚本已加浏览器 UA 和重试。
- “绝对不漏”在开放网页场景无法数学保证，但该方案已尽可能降低漏抓概率。
