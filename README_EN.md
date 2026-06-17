# AtomCode Open API

> Convert [AtomCode](https://atomgit.com/atomgit_atomcode/atomcode) CodingPlan free model credits into a standard OpenAI API.

English | [中文](./README.md)

---

## What is this?

AtomCode provides a free CodingPlan with access to multiple LLMs including DeepSeek, GLM, and Qwen. This project wraps those credits into a standard OpenAI-compatible API, so any tool that supports the OpenAI API can use them directly.

## Supported Models

| Model | Description | Context |
|-------|-------------|---------|
| `deepseek-ai/DeepSeek-V4-Flash` | Fast inference | 1M |
| `deepseek-ai/DeepSeek-R1` | Deep reasoning | 128K |
| `deepseek-ai/DeepSeek-V3` / `V3.2` / `V4-Pro` | General | 128K |
| `zai-org/GLM-5` / `GLM-5.1` | Zhipu AI | 64K |
| `Qwen/Qwen3-235B-A22B` | Qwen large MoE | 128K |
| `MoonshotAI/Kimi-K2.6` / `K2.7-Code` | Moonshot | 128K |
| ... and more | Call `/v1/models` for full list | |

## Quick Start

### Prerequisites

- Python 3.9+
- AtomCode installed and logged in (`~/.atomcode/auth.toml` must exist)

### Install & Run

```bash
git clone https://github.com/your-username/atomcode-open-api.git
cd atomcode-open-api
pip install -e .
atomcode-open-api
```

### Use with OpenAI SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8899/v1",
    api_key="anything",
)

response = client.chat.completions.create(
    model="deepseek-ai/DeepSeek-V4-Flash",
    messages=[{"role": "user", "content": "Hello!"}],
)
print(response.choices[0].message.content)
```

### Use with curl

```bash
curl http://127.0.0.1:8899/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-ai/DeepSeek-V4-Flash","messages":[{"role":"user","content":"hello"}]}'
```

## CLI Options

```
atomcode-open-api [-p PORT] [-H HOST] [-g GATEWAY] [-v]

  -p, --port PORT      Listen port (default: 8899)
  -H, --host HOST      Listen address (default: 127.0.0.1)
  -g, --gateway URL    Gateway URL (default: https://api-ai.gitcode.com)
  -v, --verbose        Verbose logging
```

## How It Works

1. **Auth**: Reads OAuth token from `~/.atomcode/auth.toml`
2. **Signing**: Adds `X-AtomCode-*` signature headers to each request
3. **Model mapping**: Maps short names (e.g. `deepseek-v4-flash`) to full names (e.g. `deepseek-ai/DeepSeek-V4-Flash`) to bypass "AtomCode exclusive" restrictions
4. **Proxy**: Forwards requests to the AtomCode API gateway, returns standard OpenAI format responses

## Quota

- CodingPlan Pro: 1000 calls per 5-hour window
- Token validity: 7 days (re-login in AtomCode after expiry)
- Check `GET /health` for current usage

## License

[MIT](./LICENSE)
