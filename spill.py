"""Length-framed temporary partitions and logical decoded-payload accounting."""
import contextlib
import json
import struct
import tempfile

def encode(row):
    return json.dumps(row, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')

class Metrics:
    def __init__(self):
        self.live = 0
        self.peak = 0
        self.limit = 0
        self.reads = {}
        self.sizes = {}
        self._serial = 0

    def acquire(self, size):
        if self.live + size > self.limit:
            raise MemoryError('PAYLOAD_BUDGET_EXCEEDED')
        self.live += size
        self.peak = max(self.peak, self.live)

    def release(self, size):
        self.live -= size

class Partition:
    def __init__(self, metrics):
        self.m = metrics
        self.ident = metrics._serial
        metrics._serial += 1
        self.file = tempfile.TemporaryFile(mode='w+b')
        self.count = self.size = 0
        metrics.reads[self.ident] = 0
        metrics.sizes[self.ident] = 0

    def close(self):
        self.file.close()

    def append(self, row):
        raw = encode(row)
        self.file.seek(0, 2)
        self.file.write(struct.pack('>I', len(raw)))
        self.file.write(raw)
        self.count += 1
        self.size += len(raw)
        self.m.sizes[self.ident] = self.count

    def _blocks(self, cap):
        self.file.seek(0)
        while True:
            rows, used = [], 0
            try:
                while True:
                    pos = self.file.tell()
                    header = self.file.read(4)
                    if not header:
                        break
                    size = struct.unpack('>I', header)[0]
                    if size > cap:
                        raise ValueError('ROW_EXCEEDS_BLOCK')
                    if used + size > cap:
                        self.file.seek(pos)
                        break
                    self.m.acquire(size)
                    used += size
                    raw = self.file.read(size)
                    if len(raw) != size:
                        raise ValueError('TRUNCATED_PARTITION')
                    rows.append(json.loads(raw))
                    self.m.reads[self.ident] += 1
                if not rows:
                    return
                yield rows
            finally:
                rows.clear()
                self.m.release(used)

    @contextlib.contextmanager
    def blocks(self, cap):
        iterator = self._blocks(cap)
        try:
            yield iterator
        finally:
            iterator.close()
