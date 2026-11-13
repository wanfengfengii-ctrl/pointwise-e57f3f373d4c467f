"""Spilling full outer reconciliation join. No output ordering guarantee."""
import contextlib
import hashlib
import tempfile
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
    # Hot-key / depth-limit fallback: a block nested loop with global match
    # state so a match found in any block pair is never reported as missing.
    # A per-block-pair full outer join would wrongly emit unmatched-side rows
    # whenever matching rows land in different blocks.
    #
    # Pick the smaller partition as the outer side to minimize inner rescans.
    # The outer side is scanned exactly once; every outer block scans the inner
    # partition once, plus one final inner sweep to emit genuinely unmatched
    # inner rows. Match markers live in a length-framed temp file (disk state is
    # allowed to grow with the input), while in-memory helper state stays bound
    # to the current block's capacity.
    if left.size <= right.size:
        outer, inner, outer_is_left = left, right, True
    else:
        outer, inner, outer_is_left = right, left, False
    cap = budget // 2
    marker = tempfile.TemporaryFile(mode='w+b')
    try:
        with outer.blocks(cap) as outer_blocks:
            for a in outer_blocks:
                # Only outer rows in the current block need in-memory match state.
                outer_hit = [False] * len(a)
                with inner.blocks(cap) as inner_blocks:
                    inner_pos = 0
                    for b in inner_blocks:
                        index = {}
                        for j, row in enumerate(b):
                            if row['key'] is not None:
                                index.setdefault(row['key'], []).append(j)
                        for ai, arow in enumerate(a):
                            if arow['key'] is None:
                                continue
                            for j in index.get(arow['key'], ()):
                                brow = b[j]
                                outer_hit[ai] = True
                                global_pos = inner_pos + j
                                marker.seek(global_pos)
                                marker.write(b'\x01')
                                if outer_is_left:
                                    yield project(arow, brow)
                                else:
                                    yield project(brow, arow)
                        inner_pos += len(b)
                for ai, arow in enumerate(a):
                    if not outer_hit[ai]:
                        if outer_is_left:
                            yield project(arow, None)
                        else:
                            yield project(None, arow)
        # Final inner sweep: emit each inner row that never matched any outer row.
        marker.seek(0)
        with inner.blocks(cap) as inner_blocks:
            inner_pos = 0
            for b in inner_blocks:
                for j, brow in enumerate(b):
                    flag = marker.read(1)
                    if flag != b'\x01':
                        if outer_is_left:
                            yield project(None, brow)
                        else:
                            yield project(brow, None)
                inner_pos += len(b)
    finally:
        marker.close()

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
