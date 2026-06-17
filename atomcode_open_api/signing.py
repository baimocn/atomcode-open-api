"""
signing - AtomCode 请求签名实现

实现 atomcode-signing-v1 签名方案：
  - 密钥派生: HKDF-SHA256 (salt=atomcode-signing-v1, info=v1:)
  - 签名算法: HMAC-SHA256
  - 消息格式: {method}\n{path}\n{sha256(body)}\n{timestamp}\n{nonce}

注意: api-ai.gitcode.com 网关当前不验证签名值，只检查头是否存在。
此实现保留用于未来可能启用签名验证的场景。
"""

import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from typing import Dict

# HKDF 参数
HKDF_SALT = b"atomcode-signing-v1"
HKDF_INFO = b"v1:"
HKDF_LENGTH = 32  # SHA-256 输出长度

# 签名算法标识
ALGORITHM = "atomcode-signing-v1"
VERSION = "v1"


@dataclass
class SignInput:
    """签名输入"""
    method: str
    path: str
    body: bytes
    oauth_token: str
    timestamp: int
    nonce: str


def derive_signing_key(oauth_token: str) -> bytes:
    """
    使用 HKDF-SHA256 从 OAuth token 派生签名密钥

    Args:
        oauth_token: AtomCode 的 access_token

    Returns:
        32 字节签名密钥
    """
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    from cryptography.hazmat.primitives import hashes

    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=HKDF_LENGTH,
        salt=HKDF_SALT,
        info=HKDF_INFO,
    )
    return hkdf.derive(oauth_token.encode("utf-8"))


def compute_signature(signing_key: bytes, method: str, path: str,
                      body_hash: str, timestamp: str, nonce: str) -> str:
    """
    计算 HMAC-SHA256 签名

    Args:
        signing_key: HKDF 派生的签名密钥
        method: HTTP 方法 (POST)
        path: 请求路径 (/v1/chat/completions)
        body_hash: 请求体的 SHA-256 十六进制摘要
        timestamp: Unix 时间戳字符串
        nonce: 随机 nonce 十六进制字符串

    Returns:
        签名的十六进制字符串
    """
    message = f"{method}\n{path}\n{body_hash}\n{timestamp}\n{nonce}"
    return hmac.new(
        signing_key,
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def generate_nonce() -> str:
    """生成 16 字节随机 nonce（十六进制）"""
    return os.urandom(16).hex()


def sign_request(method: str, path: str, body: bytes, oauth_token: str) -> Dict[str, str]:
    """
    为请求生成签名头

    Args:
        method: HTTP 方法
        path: 请求路径
        body: 请求体字节
        oauth_token: OAuth access_token

    Returns:
        包含签名头的字典
    """
    timestamp = str(int(time.time()))
    nonce = generate_nonce()
    body_hash = hashlib.sha256(body).hexdigest()

    signing_key = derive_signing_key(oauth_token)
    signature = compute_signature(signing_key, method, path, body_hash, timestamp, nonce)

    return {
        "X-AtomCode-Sig": signature,
        "X-AtomCode-Ts": timestamp,
        "X-AtomCode-Nonce": nonce,
        "X-AtomCode-Alg": ALGORITHM,
        "X-AtomCode-Ver": VERSION,
    }


def fake_sign_headers() -> Dict[str, str]:
    """
    生成假签名头（仅用于 api-ai.gitcode.com，该网关不验证签名值）

    Returns:
        包含签名头的字典（值为占位符）
    """
    return {
        "X-AtomCode-Sig": "atomcode-open-api",
        "X-AtomCode-Ts": str(int(time.time())),
        "X-AtomCode-Nonce": generate_nonce(),
        "X-AtomCode-Alg": ALGORITHM,
        "X-AtomCode-Ver": VERSION,
    }
