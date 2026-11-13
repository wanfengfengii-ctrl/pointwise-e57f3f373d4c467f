"""Spilling full outer reconciliation join. No output ordering guarantee."""
import contextlib
import hashlib
from spill import Metrics, Partition, encode

def bucket(key, depth, fanout):
    raw = (str(depth) + ':' + ('' if key is None else key)).encode('utf-8')
    return int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), 'big') % fanout

def project(left, right):
    return [None if left is None else left['id'],
            None if left is None else left['payload'],
            None if right is None else right['id'],
            None if right is None else right['payload']]

def local_outer(left, right):
    index = {}
    for j, row in enumerate(right):
        if row['key'] is not None:
            index.setdefault(row['key'], []).append(j)
    hit = set()
    for row in left:
        matches = [] if row['key'] is None else index.get(row['key'], [])
        if not matches:
            yield project(row, None)
        for j in matches:
            hit.add(j)
            yield project(row, right[j])
    for j, row in enumerate(right):
        if j not in hit:
            yield project(None, row)

def tiled_outer(left, right, budget):
    # Reuse the in-memory operator for each pair of bounded blocks.
    cap = budget // 2
    with left.blocks(cap) as left_blocks:
        for a in left_blocks:
            with right.blocks(cap) as right_blocks:
                for b in right_blocks:
                    yield from local_outer(a, b)

def solve(left, right, budget, fanout, depth, metrics):
    if not left.count or not right.count:
        source = right if not left.count else left
        with source.blocks(budget // 2) as blocks:
            for rows in blocks:
                for row in rows:
                    yield project(None, row) if not left.count else project(row, None)
        return
    if left.size + right.size <= budget:
        with left.blocks(budget) as lb:
            a = next(lb)
            with right.blocks(budget - left.size) as rb:
                b = next(rb)
                yield from local_outer(a, b)
        return
    if depth == 6:
        yield from tiled_outer(left, right, budget)
        return
    with contextlib.ExitStack() as partitions:
        parts = []
        for i in range(fanout):
            a, b = Partition(metrics), Partition(metrics)
            partitions.callback(a.close)
            partitions.callback(b.close)
            parts.append((a, b))
        for source, side in ((left, 0), (right, 1)):
            with source.blocks(budget // 2) as blocks:
                for rows in blocks:
                    for row in rows:
                        parts[bucket(row['key'], depth, fanout)][side].append(row)
        largest = max(a.size + b.size for a, b in parts)
        if largest == left.size + right.size:
            yield from tiled_outer(left, right, budget)
        else:
            for a, b in parts:
                yield from solve(a, b, budget, fanout, depth + 1, metrics)

def join(left, right, budget, *, fanout=4, metrics=None):
    if type(budget) is not int or budget < 512:
        raise ValueError('INVALID_BUDGET')
    if type(fanout) is not int or not 2 <= fanout <= 8:
        raise ValueError('INVALID_FANOUT')
    m = metrics if metrics is not None else Metrics()
    m.limit = budget
    with contextlib.ExitStack() as stack:
        a, b = Partition(m), Partition(m)
        stack.callback(a.close)
        stack.callback(b.close)
        for source, dest in ((left, a), (right, b)):
            for row in source:
                if (not isinstance(row, dict) or set(row) != {'id', 'key', 'payload'}
                    or not isinstance(row['id'], str) or not row['id']
                    or not (row['key'] is None or isinstance(row['key'], str))
                    or not isinstance(row['payload'], str)):
                    raise ValueError('INVALID_ROW')
                size = len(encode(row))
                if size > budget // 4:
                    raise ValueError('ROW_TOO_LARGE')
                m.acquire(size)
                try:
                    dest.append(row)
                finally:
                    m.release(size)
        yield from solve(a, b, budget, fanout, 0, m)
