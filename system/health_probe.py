"""Container runtime-loop probe. Worker capacity is deliberately excluded."""
import argparse
import json
from urllib.error import URLError
from urllib.request import urlopen


def check(url, timeout=2):
    """Fail closed on HTTP errors, timeouts, oversized or malformed bodies."""
    try:
        with urlopen(url, timeout=timeout) as response:
            body = response.read(65537)
        if len(body) > 65536:
            return False
        payload = json.loads(body)
        return isinstance(payload, dict) and payload.get('healthy') is True
    except (URLError, OSError, ValueError):
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8000/health/runtime')
    args = parser.parse_args()
    return 0 if check(args.url) else 1


if __name__ == '__main__':
    raise SystemExit(main())
