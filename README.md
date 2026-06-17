# AtomCode Open API

> 将 [AtomCode](https://atomgit.com/atomgit_atomcode/atomcode) CodingPlan 的免费模型额度，转为标准 OpenAI API。

[English](./README_EN.md) | 中文

---

## 这是什么？

AtomCode 提供免费的 CodingPlan，包含 DeepSeek、GLM、Qwen 等多个大模型的调用额度。本项目将这些额度包装成标准的 OpenAI 兼容 API，让任何支持 OpenAI API 的工具都能直接使用。

## 支持的模型

| 模型 | 说明 | 上下文 |
|------|------|--------|
| `deepseek-ai/DeepSeek-V4-Flash` | 快速推理 | 1M |
| `deepseek-ai/DeepSeek-R1` | 深度推理 | 128K |
| `deepseek-ai/DeepSeek-V3` / `V3.2` / `V4-Pro` | 通用对话 | 128K |
| `zai-org/GLM-5` / `GLM-5.1` | 智谱清言 | 64K |
| `Qwen/Qwen3-235B-A22B` | 通义千问大 MoE | 128K |
| `MoonshotAI/Kimi-K2.6` / `K2.7-Code` | 月之暗面 | 128K |
| ... 更多模型 | 调用 `/v1/models` 查看完整列表 | |

## 快速开始

### 1. 前置条件

- Python 3.9+
- 已安装并登录 AtomCode（需要 `~/.atomcode/auth.toml`）

### 2. 安装

```bash
git clone https://github.com/baimocn/atomcode-open-api.git
cd atomcode-open-api
pip install -e .
```

### 3. 启动

```bash
atomcode-open-api
```

输出：
```
============================================================
  AtomCode Open API v1.0.0
============================================================
  Local:   http://127.0.0.1:8899
  Gateway: https://api-ai.gitcode.com
  Models:  26 available
  Token:   expires in 6d 23h
============================================================

  Use with any OpenAI-compatible client:
    base_url = http://127.0.0.1:8899/v1
    api_key  = any-string
```

### 4. 使用

#### Python (openai SDK)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8899/v1",
    api_key="anything",  # 随便填
)

response = client.chat.completions.create(
    model="deepseek-ai/DeepSeek-V4-Flash",
    messages=[{"role": "user", "content": "用一句话介绍你自己"}],
)
print(response.choices[0].message.content)
```

#### curl

```bash
curl http://127.0.0.1:8899/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek-ai/DeepSeek-V4-Flash",
    "messages": [{"role": "user", "content": "hello"}]
  }'
```

#### Claude Code / Cursor / 任何 OpenAI 兼容工具

```yaml
# 配置 OpenAI 兼容的 API endpoint
base_url: http://127.0.0.1:8899/v1
api_key: any-string
model: deepseek-ai/DeepSeek-V4-Flash
```

### 流式输出

```python
stream = client.chat.completions.create(
    model="deepseek-ai/DeepSeek-V4-Flash",
    messages=[{"role": "user", "content": "写一首诗"}],
    stream=True,
)
for chunk in stream:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")
```

## 命令行参数

```
atomcode-open-api [-p PORT] [-H HOST] [-g GATEWAY] [-v]

参数:
  -p, --port PORT      监听端口 (default: 8899)
  -H, --host HOST      监听地址 (default: 127.0.0.1)
  -g, --gateway URL    网关地址 (default: https://api-ai.gitcode.com)
  -v, --verbose        输出详细日志
  --version            显示版本
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/v1/models` | 列出可用模型 |
| `POST` | `/v1/chat/completions` | 聊天补全（支持流式） |
| `GET` | `/health` | 健康检查和统计 |
| `GET` | `/` | Web 状态页 |

## 工作原理

```
┌─────────────────┐     OpenAI API      ┌─────────────────┐    带签名的请求    ┌──────────────────┐
│  你的应用/工具    │ ─────────────────→  │  atomcode-open-  │ ────────────────→ │ api-ai.gitcode.com│
│  (Claude Code,   │  POST /v1/chat/... │  api (本项目)     │  + X-AtomCode-*  │  (AtomCode 网关)  │
│   Cursor, etc.)  │ ←───────────────── │  port 8899       │ ←──────────────── │                  │
└─────────────────┘     标准响应          └─────────────────┘    网关响应         └──────────────────┘
                              │                    │
                              │              ┌─────┴─────┐
                              │              │  自动处理:  │
                              │              │ • 读取 token│
                              │              │ • 请求签名  │
                              │              │ • 模型映射  │
                              │              └───────────┘
```

1. **认证**: 从 `~/.atomcode/auth.toml` 读取 OAuth token
2. **签名**: 为每个请求添加 `X-AtomCode-*` 签名头
3. **模型映射**: 将短名称（如 `deepseek-v4-flash`）映射为全名（如 `deepseek-ai/DeepSeek-V4-Flash`），绕过 "AtomCode 独享" 限制
4. **代理转发**: 将请求转发到 AtomCode API 网关，返回标准 OpenAI 格式响应

## 配额说明

- CodingPlan Pro: 每 5 小时 1000 次调用
- Token 有效期: 7 天（过期后需在 AtomCode 中重新登录）
- 调用 `GET /health` 查看当前用量

## 常见问题

### Token 过期了怎么办？

在 AtomCode 中运行 `/login` 重新登录，代理会自动读取新 token。

### 模型返回 "AtomCode 独享"？

确保使用 `/v1/models` 返回的完整模型名（如 `deepseek-ai/DeepSeek-V4-Flash`），不要使用短别名（如 `deepseek-v4-flash`）。

### 能从局域网其他机器访问吗？

```bash
atomcode-open-api -H 0.0.0.0
```

### 支持 function calling 吗？

支持。本项目是透传代理，所有 OpenAI API 功能（function calling、vision、streaming）都由上游模型支持。

## 开发

```bash
git clone https://github.com/baimocn/atomcode-open-api.git
cd atomcode-open-api
pip install -e ".[dev]"
python -m pytest
```

## 项目结构

```
atomcode-open-api/
├── atomcode_open_api/
│   ├── __init__.py     # 版本信息
│   ├── auth.py         # OAuth 认证读取
│   ├── signing.py      # 请求签名实现
│   ├── gateway.py      # API 网关通信
│   ├── models.py       # 模型名称映射
│   ├── server.py       # HTTP 代理服务器
│   └── cli.py          # 命令行入口
├── pyproject.toml      # 项目配置
├── README.md           # 中文文档
├── README_EN.md        # English docs
└── LICENSE             # MIT License
```

## 致谢

- [AtomCode](https://atomgit.com/atomgit_atomcode/atomcode) — 100% AI 生成的开源编码代理
- [DeepSeek](https://deepseek.com) — 强大的推理模型
- [Zhipu AI](https://zhipuai.cn) — GLM 系列模型

## 免责声明

本项目仅供学习交流使用。请遵守 AtomCode 的使用条款和 CodingPlan 的服务协议。本项目与 AtomCode 官方无关。

## License

[MIT](./LICENSE)
