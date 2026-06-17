"""
auth - 读取 AtomCode 的 OAuth 认证信息

从 ~/.atomcode/auth.toml 中加载 access_token 和 user_id，
用于向 AtomCode API 网关发送已认证的请求。
"""

import os
import sys
import time
from pathlib import Path
from typing import Optional

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # fallback
    except ImportError:
        tomllib = None


# AtomCode 配置目录
ATOMCODE_HOME = Path(os.environ.get("ATOMCODE_HOME", Path.home() / ".atomcode"))
AUTH_FILE = ATOMCODE_HOME / "auth.toml"

# Token 提前刷新的安全余量（秒）
TOKEN_EXPIRY_MARGIN = 300  # 5 分钟


class AuthInfo:
    """AtomCode 认证信息"""

    def __init__(self, access_token: str, user_id: str, refresh_token: Optional[str] = None,
                 expires_in: Optional[int] = None, created_at: int = 0):
        self.access_token = access_token
        self.user_id = user_id
        self.refresh_token = refresh_token
        self.expires_in = expires_in
        self.created_at = created_at

    @property
    def is_expired(self) -> bool:
        """Token 是否已过期（或即将过期）"""
        if self.expires_in is None:
            return False
        now = int(time.time())
        expires_at = self.created_at + self.expires_in
        return now >= expires_at - TOKEN_EXPIRY_MARGIN

    @property
    def remaining_seconds(self) -> Optional[int]:
        """Token 剩余有效秒数"""
        if self.expires_in is None:
            return None
        expires_at = self.created_at + self.expires_in
        remaining = expires_at - int(time.time())
        return max(0, remaining)

    @property
    def remaining_display(self) -> str:
        """Token 剩余时间的人类可读格式"""
        secs = self.remaining_seconds
        if secs is None:
            return "unknown"
        if secs < 60:
            return f"{secs}s"
        if secs < 3600:
            return f"{secs // 60}m"
        hours = secs // 3600
        days = hours // 24
        if days > 0:
            return f"{days}d {hours % 24}h"
        return f"{hours}h {secs % 3600 // 60}m"


def load_auth() -> Optional[AuthInfo]:
    """
    从 ~/.atomcode/auth.toml 加载认证信息

    Returns:
        AuthInfo 或 None（文件不存在或格式错误）
    """
    if not AUTH_FILE.exists():
        return None

    if tomllib is None:
        # 简单的 TOML 解析（只处理 auth.toml 的扁平结构）
        return _parse_auth_simple()

    try:
        with open(AUTH_FILE, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        return _parse_auth_simple()

    access_token = data.get("access_token", "")
    if not access_token:
        return None

    user = data.get("user", {})
    return AuthInfo(
        access_token=access_token,
        user_id=user.get("id", ""),
        refresh_token=data.get("refresh_token"),
        expires_in=data.get("expires_in"),
        created_at=data.get("created_at", 0),
    )


def _parse_auth_simple() -> Optional[AuthInfo]:
    """简单的 TOML 解析（不依赖 tomllib）"""
    try:
        content = AUTH_FILE.read_text(encoding="utf-8")
    except Exception:
        return None

    values = {}
    for line in content.split("\n"):
        line = line.strip()
        if "=" in line and not line.startswith("#") and not line.startswith("["):
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"')
            values[key] = value

    access_token = values.get("access_token", "")
    if not access_token:
        return None

    expires_in = None
    if "expires_in" in values:
        try:
            expires_in = int(values["expires_in"])
        except ValueError:
            pass

    created_at = 0
    if "created_at" in values:
        try:
            created_at = int(values["created_at"])
        except ValueError:
            pass

    return AuthInfo(
        access_token=access_token,
        user_id=values.get("id", ""),
        refresh_token=values.get("refresh_token"),
        expires_in=expires_in,
        created_at=created_at,
    )


def require_auth() -> AuthInfo:
    """
    加载认证信息，失败时退出程序

    Returns:
        AuthInfo（有效）
    """
    auth = load_auth()
    if auth is None:
        print(f"错误: 未找到 AtomCode 认证信息", file=sys.stderr)
        print(f"请先运行 AtomCode 并登录: atomcode login", file=sys.stderr)
        print(f"认证文件位置: {AUTH_FILE}", file=sys.stderr)
        sys.exit(1)

    if auth.is_expired:
        print(f"警告: Token 已过期，可能需要重新登录", file=sys.stderr)
        print(f"请在 AtomCode 中运行 /login 重新登录", file=sys.stderr)

    return auth
