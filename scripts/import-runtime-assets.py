#!/usr/bin/env python3
"""Install the checksum-verified runtime dependencies."""
import argparse
import hashlib
import json
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def install(archive):
    expected = json.loads((ROOT / 'assets/runtime-manifest.json').read_text())['files']
    with zipfile.ZipFile(archive) as z:
        for item in expected:
            path = item['path']
            if not path.startswith('runtime/') or not (ROOT / path).resolve().is_relative_to((ROOT / 'runtime').resolve()):
                raise ValueError('Unsafe runtime asset path')
            info = z.getinfo(path)
            if info.file_size != item['size_bytes']:
                raise ValueError(f'Unexpected size: {path}')
            # Validate everything before changing the installed asset tree.
            h = hashlib.sha256()
            with z.open(path) as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    h.update(block)
            if h.hexdigest() != item['sha256']:
                raise ValueError(f'Checksum mismatch: {path}')
        for item in expected:
            dest = ROOT / item['path']
            dest.parent.mkdir(parents=True, exist_ok=True)
            with z.open(item['path']) as src, dest.open('wb') as out:
                shutil.copyfileobj(src, out)
    print(f'Installed {len(expected)} checksum-verified runtime assets in {ROOT / "runtime"}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', nargs='?', type=Path)
    parser.add_argument('--url', help='HTTPS companion-asset URL from the same tagged release')
    args = parser.parse_args()
    if bool(args.archive) == bool(args.url):
        parser.error('Supply an archive path or --url, but not both')
    if args.url:
        if not args.url.startswith('https://'):
            parser.error('--url must use HTTPS')
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / 'runtime-assets.zip'
            urllib.request.urlretrieve(args.url, archive)
            install(archive)
    else:
        install(args.archive)
