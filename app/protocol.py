"""mkvbase.site protocol — reverse-engineered from their Next.js client bundles.

Session handshake (GET /api/links with no query):
  Server sets cookies:
    mkv_client_key = 64 hex chars                 -> HMAC secret for this browser
    mkv_challenge  = urlenc("<salt>:<diff>:<expiry_ms>:<hmac>")
    mkv_seq        = "1"                          -> replay counter

Search (GET /api/links?q=..&t=..&seq=..&pow=..&ent=..&sig=..):
  1. q   = hex( utf8(term) XOR (t % 256) )         t = Date.now()
  2. pow = nonce n with sha256(f"{salt}:{n}") -> h1,
           sha256(f"{h1}:{q_hex}") starting with "0"*difficulty   (seen 2-3)
  3. sig = HMAC-SHA256(client_key, f"{q_hex}:{t}:{seq}:{n}:{ent}").hex()
  4. ent = interaction entropy (10 is what their client sends)
"""
from __future__ import annotations

import hashlib
import hmac
import time
import urllib.parse


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def hmac_sha256_hex(key: str, msg: str) -> str:
    return hmac.new(key.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()


def xor_encode_hex(term: str, ts_ms: int) -> str:
    """term -> hex, each utf8 byte XORed with (ts_ms % 256)."""
    key = ts_ms % 256
    return bytes(b ^ key for b in term.encode("utf-8")).hex()


def xor_decode_hex(q_hex: str, ts_ms: int) -> str:
    key = ts_ms % 256
    return bytes(b ^ key for b in bytes.fromhex(q_hex)).decode("utf-8")


def solve_pow(salt: str, difficulty: int, data_hex: str, max_iters: int = 2_000_000) -> int:
    """Return nonce such that sha256(sha256(f"{salt}:{n}") + ":" + q) starts with "0"*diff."""
    prefix = "0" * difficulty
    for nonce in range(max_iters):
        h1 = sha256_hex(f"{salt}:{nonce}")
        if sha256_hex(f"{h1}:{data_hex}").startswith(prefix):
            return nonce
    raise RuntimeError(f"proof-of-work exhausted ({max_iters} iterations, difficulty {difficulty})")


def search_signature(client_key: str, q_hex: str, ts_ms: int, seq: str | int, nonce: int, ent: int) -> str:
    return hmac_sha256_hex(client_key, f"{q_hex}:{ts_ms}:{seq}:{nonce}:{ent}")


def parse_challenge(challenge: str) -> tuple[str, int]:
    """mkv_challenge cookie -> (salt, difficulty). Raises ValueError if malformed."""
    challenge = urllib.parse.unquote(challenge)
    parts = challenge.split(":")
    if len(parts) != 4:
        raise ValueError(f"bad challenge shape: {challenge[:40]}...")
    return parts[0], int(parts[1])


def build_search_url(base: str, term: str, client_key: str, seq: str | int,
                     challenge: str, ent: int = 10, ts_ms: int | None = None) -> str:
    """Full pipeline: term -> signed search URL ready to open in the browser."""
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    salt, difficulty = parse_challenge(challenge)
    q_hex = xor_encode_hex(term, ts_ms)
    nonce = solve_pow(salt, difficulty, q_hex)
    sig = search_signature(client_key, q_hex, ts_ms, seq, nonce, ent)
    return (f"{base.rstrip('/')}/api/links?q={q_hex}&t={ts_ms}"
            f"&seq={seq}&pow={nonce}&ent={ent}&sig={sig}")
