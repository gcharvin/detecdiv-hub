"""Read only central directories of legacy ZIPs identified by Hub history."""

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import time
import zipfile


WORK = Path(__file__).resolve().parent
LIMIT_PER_ARCHIVE = 16 * 1024 * 1024
LIMIT_TOTAL = 256 * 1024 * 1024


class IndexBudgetExceeded(ValueError):
    pass


class BoundedReader:
    def __init__(self, stream, limit, size):
        self.stream = stream
        self.remaining = limit
        self.size = size

    def seek(self, *args):
        return self.stream.seek(*args)

    def tell(self):
        return self.stream.tell()

    def seekable(self):
        return True

    def read(self, count=-1):
        if count < 0:
            count = max(0, self.size - self.tell())
        if count > self.remaining:
            raise IndexBudgetExceeded('ZIP central directory exceeds the read budget')
        data = self.stream.read(count)
        self.remaining -= len(data)
        return data


def normalized(stem):
    return re.sub('[^a-z0-9]+', '', stem.casefold())


def stamp(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def main():
    os.umask(0o077)
    os.nice(19)
    request_path = WORK / 'request.json'
    request_bytes = request_path.read_bytes()
    request = json.loads(request_bytes)
    assert request['read_only'] and not request['extract_members']
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == request['script_sha256']
    archives = request['archives']
    assert len(archives) == 1 and len({item['path'] for item in archives}) == len(archives)
    assert sum(len(item['projects']) for item in archives) == 1
    acquisitions = {name.casefold() for name in request['acquisitions']}
    counts = Counter()
    consumed = 0
    deadline = time.monotonic() + 1800
    receipt = WORK / 'receipt.jsonl'
    with receipt.open('x', encoding='utf-8') as output:
        for number, item in enumerate(archives, 1):
            assert time.monotonic() < deadline
            archive_path = Path(item['path'])
            assert archive_path.parts[:3] in (('/', 'archive', 'theo'), ('/', 'archive', 'audrey')) and archive_path.suffix.casefold() == '.zip'
            assert '..' not in archive_path.parts
            names = {normalized(Path(name).stem) for p in item['projects'] for name in p['declared_names']}
            result = {'archive': str(archive_path), 'projects': [p['id'] for p in item['projects']], 'candidates': []}
            try:
                assert archive_path.parent.resolve(strict=True) == archive_path.parent
                fd = os.open(archive_path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, 'rb') as stream:
                    before = os.fstat(stream.fileno())
                    assert stat.S_ISREG(before.st_mode)
                    budget = min(LIMIT_PER_ARCHIVE, LIMIT_TOTAL - consumed)
                    if budget <= 0:
                        raise IndexBudgetExceeded('Total ZIP index budget exhausted')
                    reader = BoundedReader(stream, budget, before.st_size)
                    try:
                        with zipfile.ZipFile(reader, 'r') as archive:
                            entries = archive.infolist()
                            result['entries'] = len(entries)
                            for entry in entries:
                                path = PurePosixPath(entry.filename.replace(chr(92), '/'))
                                if entry.is_dir() or '..' in path.parts or path.is_absolute():
                                    continue
                                segments = {part.casefold() for part in path.parts}
                                name_match = path.suffix.casefold() in ('.mat', '.json') and normalized(path.stem) in names
                                acquisition_matches = sorted(segments & acquisitions)
                                if name_match or acquisition_matches:
                                    if len(result['candidates']) >= 100:
                                        raise IndexBudgetExceeded('Candidate reporting cap reached')
                                    result['candidates'].append({
                                        'member': str(path), 'bytes': entry.file_size,
                                        'stored_bytes': entry.compress_size, 'crc32': f'{entry.CRC:08x}',
                                        'main_name_match': name_match,
                                        'acquisition_segments': acquisition_matches,
                                    })
                    finally:
                        consumed += budget - reader.remaining
                    assert stamp(before) == stamp(os.fstat(stream.fileno()))
                result['status'] = 'complete_index_only'
            except (OSError, ValueError, zipfile.BadZipFile) as error:
                result['status'] = type(error).__name__
            counts[result['status']] += 1
            counts['entries'] += result.get('entries', 0)
            counts['candidates'] += len(result['candidates'])
            counts['archives_with_candidates'] += bool(result['candidates'])
            output.write(json.dumps(result) + '\n')
            output.flush()
            if number % 20 == 0:
                (WORK / 'progress.json').write_text(json.dumps({'archives_checked': number, 'counts': dict(counts)}) + '\n')
        os.fsync(output.fileno())
    result = {
        'success': True, 'checked_at': datetime.now(timezone.utc).isoformat(),
        'read_only': True, 'archives_checked': len(archives), 'counts': dict(counts),
        'index_bytes_read': consumed, 'member_payload_reads': 0,
        'request_sha256': hashlib.sha256(request_bytes).hexdigest(),
        'receipt_sha256': hashlib.sha256(receipt.read_bytes()).hexdigest(),
        'source_deleted': False,
    }
    with (WORK / 'result.json').open('x', encoding='utf-8') as output:
        json.dump(result, output, indent=2)
        output.write('\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
