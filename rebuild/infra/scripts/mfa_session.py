#!/usr/bin/env python3
"""Get an MFA session (sts:GetSessionToken) with boto3 and write it as a profile in the shared credentials file.

REVIEW4_OPERATIONS B4: the operator's session script called the AWS CLI, which does not run on this machine. This replaces it.
It names no profile, account or organisation: every name comes from the command line.

  mfa_session.py --source-profile SRC --target-profile DST --otp 123456 [--duration 129600] [--serial ARN]

1. The MFA device ARN is --serial, or `mfa_serial` of profile SRC in the shared config file (~/.aws/config, or AWS_CONFIG_FILE).
2. sts.get_session_token(SerialNumber, TokenCode, DurationSeconds) runs with the credentials of profile SRC.
3. Profile DST in the shared credentials file (~/.aws/credentials, or AWS_SHARED_CREDENTIALS_FILE) gets aws_access_key_id,
   aws_secret_access_key and aws_session_token. Every other profile and key in the file is kept (comments are not: the file is
   rewritten by configparser). The file is replaced atomically with mode 0600.
Refusals: DST equal to SRC; DST holding long-term keys (keys and no session token), so long-term keys are never overwritten;
an OTP that is not 6 digits; a duration outside 900 to 129600 s. No secret is printed. Exit 0 ok, 1 STS refused, 2 bad usage.
"""
from __future__ import annotations

import argparse
import configparser
import datetime as dt
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Callable


def default_paths() -> tuple[Path, Path]:
    cfg = Path(os.environ.get("AWS_CONFIG_FILE", Path.home() / ".aws" / "config")).expanduser()
    cred = Path(os.environ.get("AWS_SHARED_CREDENTIALS_FILE", Path.home() / ".aws" / "credentials")).expanduser()
    return cfg, cred


def mfa_serial_from_config(config_file: Path, profile: str) -> str | None:
    cp = configparser.RawConfigParser()
    cp.read(config_file)
    for section in ((f"profile {profile}", profile) if profile != "default" else ("default", "profile default")):
        if cp.has_section(section) and cp.has_option(section, "mfa_serial"):
            return cp.get(section, "mfa_serial").strip()
    return None


def write_profile(cred_file: Path, profile: str, creds: dict) -> None:
    cp = configparser.RawConfigParser()
    cp.optionxform = str  # keep the case of keys exactly as found
    if cred_file.exists():
        cp.read(cred_file)
    if cp.has_section(profile):
        keys = set(cp.options(profile))
        if {"aws_access_key_id", "aws_secret_access_key"} <= keys and "aws_session_token" not in keys:
            raise SystemExit(f"refusing: profile {profile!r} holds long-term keys (no session token); choose another target profile")
        cp.remove_section(profile)
    cp.add_section(profile)
    cp.set(profile, "aws_access_key_id", creds["AccessKeyId"])
    cp.set(profile, "aws_secret_access_key", creds["SecretAccessKey"])
    cp.set(profile, "aws_session_token", creds["SessionToken"])
    cred_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, tmp = tempfile.mkstemp(prefix=".credentials.", dir=cred_file.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            cp.write(f)
        os.replace(tmp, cred_file)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def get_session(sts, serial: str, otp: str, duration: int) -> dict:
    return sts.get_session_token(SerialNumber=serial, TokenCode=otp, DurationSeconds=duration)["Credentials"]


def main(argv: list[str] | None = None, sts_factory: Callable[[str], object] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source-profile", required=True, help="profile with the long-term credentials and mfa_serial")
    ap.add_argument("--target-profile", required=True, help="profile that receives the session credentials")
    ap.add_argument("--otp", required=True, help="the 6-digit one-time password")
    ap.add_argument("--duration", type=int, default=129600, help="seconds, 900 to 129600 (36 h, the IAM user maximum)")
    ap.add_argument("--serial", default=None, help="MFA device ARN; default: mfa_serial of the source profile")
    ap.add_argument("--config-file", type=Path, default=None)
    ap.add_argument("--credentials-file", type=Path, default=None)
    a = ap.parse_args(argv)
    cfg_default, cred_default = default_paths()
    config_file, cred_file = a.config_file or cfg_default, a.credentials_file or cred_default
    if a.source_profile == a.target_profile:
        print("refusing: the target profile must differ from the source profile", file=sys.stderr)
        return 2
    if not re.fullmatch(r"[0-9]{6}", a.otp):
        print("refusing: the one-time password must be 6 digits", file=sys.stderr)
        return 2
    if not 900 <= a.duration <= 129600:
        print("refusing: --duration must be 900 to 129600 seconds", file=sys.stderr)
        return 2
    serial = a.serial or mfa_serial_from_config(config_file, a.source_profile)
    if not serial:
        print(f"no --serial and no mfa_serial for the source profile in {config_file}", file=sys.stderr)
        return 2
    if sts_factory is None:
        import boto3

        def sts_factory(profile: str):
            return boto3.Session(profile_name=profile).client("sts")
    try:
        creds = get_session(sts_factory(a.source_profile), serial, a.otp, a.duration)
    except Exception as e:  # noqa: BLE001  botocore ClientError and credential errors alike
        print(f"STS refused the session: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
        return 1
    write_profile(cred_file, a.target_profile, creds)
    exp = creds["Expiration"]
    if isinstance(exp, dt.datetime):
        exp = exp.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"wrote profile {a.target_profile} to {cred_file}; the session expires at {exp}. export AWS_PROFILE={a.target_profile}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
