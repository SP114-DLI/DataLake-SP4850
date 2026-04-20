"""MinIO / S3 connection diagnostics."""

import sys
import socket
import urllib3

# Suppress InsecureRequestWarning for http tests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from raw.config import ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET_RAW as BUCKET

DIVIDER = "-" * 60


def test_dns():
    """Test DNS resolution."""
    print(f"\n1. DNS Resolution: {ENDPOINT}")
    print(DIVIDER)
    try:
        ip = socket.gethostbyname(ENDPOINT)
        print(f"   PASS - resolved to {ip}")
        return True
    except socket.gaierror as e:
        print(f"   FAIL - {e}")
        return False


def test_tcp(port):
    """Test raw TCP connection on a given port."""
    print(f"\n2. TCP Connect: {ENDPOINT}:{port}")
    print(DIVIDER)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((ENDPOINT, port))
        print(f"   PASS - port {port} is open")
        return True
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        print(f"   FAIL - {e}")
        return False
    finally:
        sock.close()


def test_http_raw():
    """Test a plain HTTP GET to see what the server actually returns."""
    import requests

    print(f"\n3. Raw HTTP probe")
    print(DIVIDER)
    for scheme in ("http", "https"):
        url = f"{scheme}://{ENDPOINT}"
        print(f"   Trying {url} ...")
        try:
            r = requests.get(url, timeout=10, verify=False, allow_redirects=False)
            print(f"   {scheme.upper()} -> {r.status_code}  headers: {dict(r.headers)}")
            if r.is_redirect:
                print(f"   REDIRECT -> {r.headers.get('Location')}")
        except requests.ConnectionError as e:
            print(f"   {scheme.upper()} -> ConnectionError: {e}")
        except Exception as e:
            print(f"   {scheme.upper()} -> {type(e).__name__}: {e}")


def test_minio_client():
    """Test using the minio Python SDK (what raw/pipeline.py uses)."""
    print(f"\n4. Minio SDK connection")
    print(DIVIDER)

    for secure in (False, True):
        label = "HTTPS" if secure else "HTTP"
        print(f"   Trying secure={secure} ({label}) ...")
        try:
            from minio import Minio
            client = Minio(
                ENDPOINT,
                access_key=ACCESS_KEY,
                secret_key=SECRET_KEY,
                secure=secure,
            )
            buckets = client.list_buckets()
            names = [b.name for b in buckets]
            print(f"   PASS ({label}) - buckets: {names}")
            return True, secure
        except RecursionError:
            print(f"   FAIL ({label}) - RecursionError (redirect loop — wrong protocol)")
        except Exception as e:
            print(f"   FAIL ({label}) - {type(e).__name__}: {e}")

    return False, None


def test_boto3_client():
    """Test using boto3 (what silver/transform.py uses)."""
    import boto3
    from botocore.config import Config as BotoConfig

    print(f"\n5. boto3 S3 client connection")
    print(DIVIDER)

    for use_ssl, scheme in [(False, "http"), (True, "https")]:
        label = scheme.upper()
        print(f"   Trying {label} ...")
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=f"{scheme}://{ENDPOINT}",
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                region_name="us-east-1",
                use_ssl=use_ssl,
                verify=False,
                config=BotoConfig(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    retries={"max_attempts": 2},
                ),
            )
            resp = s3.list_buckets()
            names = [b["Name"] for b in resp.get("Buckets", [])]
            print(f"   PASS ({label}) - buckets: {names}")

            # Try listing objects in raw bucket
            try:
                objs = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=3)
                keys = [o["Key"] for o in objs.get("Contents", [])]
                print(f"   Objects in '{BUCKET}' (first 3): {keys}")
            except Exception as e:
                print(f"   Bucket '{BUCKET}' list failed: {e}")

            return True, scheme
        except RecursionError:
            print(f"   FAIL ({label}) - RecursionError (redirect loop — wrong protocol)")
        except Exception as e:
            print(f"   FAIL ({label}) - {type(e).__name__}: {e}")

    return False, None


def main():
    print("=" * 60)
    print("  MinIO / S3 CONNECTION DIAGNOSTICS")
    print(f"  Endpoint: {ENDPOINT}")
    print("=" * 60)

    # 1 — DNS
    if not test_dns():
        print("\n** DNS failed — cannot reach server. Check endpoint or network.")
        return 1

    # 2 — TCP
    tcp_ok = test_tcp(80) or test_tcp(443)
    if not tcp_ok:
        print("\n** TCP failed on 80 and 443 — server may be down or firewalled.")
        return 1

    # 3 — Raw HTTP probe (shows redirects that cause recursion)
    test_http_raw()

    # 4 — Minio SDK
    minio_ok, minio_secure = test_minio_client()

    # 5 — boto3
    boto_ok, boto_scheme = test_boto3_client()

    # Summary
    print(f"\n{'=' * 60}")
    print("  SUMMARY")
    print(f"{'=' * 60}")
    if minio_ok:
        print(f"  Minio SDK works with secure={minio_secure}")
    else:
        print("  Minio SDK: FAILED both HTTP and HTTPS")
    if boto_ok:
        print(f"  boto3 works with {boto_scheme}://")
    else:
        print("  boto3: FAILED both HTTP and HTTPS")

    if minio_ok or boto_ok:
        working = "HTTPS" if (minio_secure or boto_scheme == "https") else "HTTP"
        print(f"\n  --> Server wants {working}.")
        if working == "HTTPS":
            print("  --> Set USE_HTTPS = True in raw/config.py")
        else:
            print("  --> Set USE_HTTPS = False in raw/config.py (current setting)")
        return 0
    else:
        print("\n  --> Both clients failed. Check:")
        print("      1. Is the loclx tunnel running?")
        print("      2. Are credentials correct?")
        print("      3. Does the server require HTTPS but redirect HTTP?")
        return 1


if __name__ == "__main__":
    sys.exit(main())