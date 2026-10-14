#!/usr/bin/env python3
"""Offline test of scripts/mfa_session.py (REVIEW4 B4) with a botocore Stubber on STS and temporary config files. No AWS call:
the credentials are fake and the stubbed client never reaches the network."""
import configparser
import datetime as dt
import os
import stat
import sys
import tempfile
from pathlib import Path

os.environ.update(AWS_ACCESS_KEY_ID="AKIAFAKEFAKEFAKEFAKE", AWS_SECRET_ACCESS_KEY="fake", AWS_DEFAULT_REGION="us-east-2")
os.environ.pop("AWS_PROFILE", None)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import boto3  # noqa: E402
from botocore.stub import Stubber  # noqa: E402

import mfa_session  # noqa: E402

fails = 0


def check(name, cond, extra=""):
    global fails
    print(("ok   " if cond else "FAIL ") + name + ("" if cond else f"  {extra}"))
    fails += 0 if cond else 1


d = Path(tempfile.mkdtemp())
cfg, cred = d / "config", d / "credentials"
SERIAL = "arn:aws:iam::000000000000:mfa/operator"
cfg.write_text(f"[profile src]\nregion = us-east-2\nmfa_serial = {SERIAL}\n\n[profile other]\nregion = us-west-2\n")
cred.write_text("[src]\naws_access_key_id = AKIALONGTERMLONGTERM\naws_secret_access_key = longterm\n\n"
                "[unrelated]\naws_access_key_id = AKIAUNRELATEDUNRELAT\naws_secret_access_key = keepme\n\n"
                "[dst]\naws_access_key_id = ASIAOLDOLDOLDOLDOLD1\naws_secret_access_key = old\naws_session_token = oldtoken\n")
EXP = dt.datetime(2026, 10, 15, 6, 0, tzinfo=dt.timezone.utc)
RESP = {"Credentials": {"AccessKeyId": "ASIANEWNEWNEWNEWNEW1", "SecretAccessKey": "newsecret", "SessionToken": "newtoken",
                        "Expiration": EXP}}
calls = []


def factory(want_params, response=RESP, error=None):
    def make(profile):
        calls.append(profile)
        sts = boto3.Session(region_name="us-east-2").client("sts")
        st = Stubber(sts)
        if error:
            st.add_client_error("get_session_token", service_error_code=error, http_status_code=403, expected_params=want_params)
        else:
            st.add_response("get_session_token", response, want_params)
        st.activate()
        return sts
    return make


base = ["--config-file", str(cfg), "--credentials-file", str(cred)]
want = {"SerialNumber": SERIAL, "TokenCode": "123456", "DurationSeconds": 129600}
rc = mfa_session.main(["--source-profile", "src", "--target-profile", "dst", "--otp", "123456"] + base, factory(want))
cp = configparser.RawConfigParser()
cp.read(cred)
check("session written: rc 0", rc == 0)
check("serial from mfa_serial of the source profile, 36 h default, the given OTP (stub checked the parameters)", rc == 0)
check("the STS client was made with the SOURCE profile", calls[-1] == "src", calls)
check("target profile holds the new session", dict(cp.items("dst")) == {"aws_access_key_id": "ASIANEWNEWNEWNEWNEW1",
      "aws_secret_access_key": "newsecret", "aws_session_token": "newtoken"}, dict(cp.items("dst")))
check("source profile kept unchanged", dict(cp.items("src")) == {"aws_access_key_id": "AKIALONGTERMLONGTERM", "aws_secret_access_key": "longterm"})
check("unrelated profile kept unchanged", dict(cp.items("unrelated")) == {"aws_access_key_id": "AKIAUNRELATEDUNRELAT", "aws_secret_access_key": "keepme"})
check("credentials file mode 0600", stat.S_IMODE(cred.stat().st_mode) == 0o600, oct(cred.stat().st_mode))

want2 = {"SerialNumber": "arn:aws:iam::000000000000:mfa/other", "TokenCode": "654321", "DurationSeconds": 3600}
rc = mfa_session.main(["--source-profile", "src", "--target-profile", "fresh", "--otp", "654321", "--duration", "3600",
                       "--serial", "arn:aws:iam::000000000000:mfa/other"] + base, factory(want2))
cp.read(cred)
check("--serial and --duration are passed through; a new profile is added", rc == 0 and cp.get("fresh", "aws_session_token") == "newtoken")

before = cred.read_bytes()
rc = mfa_session.main(["--source-profile", "src", "--target-profile", "src", "--otp", "123456"] + base, factory(want))
check("target equal to source refused, nothing written", rc == 2 and cred.read_bytes() == before)
rc = mfa_session.main(["--source-profile", "src", "--target-profile", "dst", "--otp", "12345"] + base, factory(want))
check("OTP that is not 6 digits refused", rc == 2 and cred.read_bytes() == before)
rc = mfa_session.main(["--source-profile", "src", "--target-profile", "dst", "--otp", "123456", "--duration", "200000"] + base, factory(want))
check("duration above 36 h refused", rc == 2 and cred.read_bytes() == before)
try:
    mfa_session.main(["--source-profile", "other", "--target-profile", "unrelated", "--otp", "123456", "--serial", SERIAL] + base,
                     factory({"SerialNumber": SERIAL, "TokenCode": "123456", "DurationSeconds": 129600}))
    check("a target profile with long-term keys is never overwritten", False)
except SystemExit as e:
    check("a target profile with long-term keys is never overwritten", "long-term" in str(e) and cred.read_bytes() == before, str(e))
rc = mfa_session.main(["--source-profile", "other", "--target-profile", "dst", "--otp", "123456"] + base, factory(want))
check("no mfa_serial and no --serial refused", rc == 2 and cred.read_bytes() == before)
rc = mfa_session.main(["--source-profile", "src", "--target-profile", "dst", "--otp", "123456"] + base, factory(want, error="AccessDenied"))
check("STS refusal: rc 1 and the file is unchanged", rc == 1 and cred.read_bytes() == before)

src = Path(mfa_session.__file__).read_text()
check("the script holds no account ID (names are checked by the forbidden-word scan of tests/check_manifests.py)",
      not __import__("re").search(r"\b[0-9]{12}\b", src))
sys.exit(1 if fails else 0)
