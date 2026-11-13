import collections
import json
import sys
from joiner import join
from samples import normal, incident, row
from spill import Metrics

def bag(rows):
    return collections.Counter(json.dumps(r, ensure_ascii=False) for r in rows)

def oracle(left, right):
    out = []
    for a in left:
        matched = False
        for b in right:
            if a['key'] is not None and a['key'] == b['key']:
                out.append([a['id'], a['payload'], b['id'], b['payload']])
                matched = True
        if not matched:
            out.append([a['id'], a['payload'], None, None])
    for b in right:
        if b['key'] is None or not any(a['key'] == b['key'] for a in left):
            out.append([None, None, b['id'], b['payload']])
    return out

def exercise(left, right, budget, marker, fanout=4):
    m = Metrics()
    actual = list(join(iter(left), iter(right), budget, fanout=fanout, metrics=m))
    expected = oracle(left, right)
    if bag(actual) != bag(expected) or m.live != 0 or m.peak > budget:
        print(marker, json.dumps({'expected': len(expected), 'actual': len(actual),
                                  'extra': list((bag(actual) - bag(expected)).elements())[:2],
                                  'missing': list((bag(expected) - bag(actual)).elements())[:2],
                                  'peak': m.peak, 'live': m.live}, ensure_ascii=False))
        return False
    print(marker + '_OK', json.dumps({'rows': len(actual), 'peak': m.peak}))
    return True

def run(mode):
    if mode == 'normal':
        ok = exercise(*normal(), 8192, 'NORMAL_REGRESSION')
        ok = exercise([], [row('r', 'a')], 512, 'EMPTY_SIDE') and ok
        ok = exercise([row('l', None)], [row('r', None)], 512, 'NULL_KEYS') and ok
        hot_left = [row('hl' + str(i), 'hot', 65) for i in range(7)]
        hot_right = [row('hr' + str(i), 'hot', 65) for i in range(6)]
        ok = exercise(hot_left, hot_right, 1024, 'HOT_DUPLICATE_REGRESSION') and ok
        return 0 if ok else 1
    if mode == 'repair':
        return 0 if exercise(*incident(), 1024, 'JOIN_SPILL_MISMATCH') else 1
    if mode == 'verify':
        a = run('normal')
        b = run('repair')
        return max(a, b)
    if mode == 'demo':
        left, right = incident()
        result = list(join(iter(left), iter(right), 1024))
        print(json.dumps({'input': {'left': left, 'right': right},
                          'output': result, 'expected_rows': 61}, ensure_ascii=False))
        return 0
    raise ValueError('UNKNOWN_MODE')

if __name__ == '__main__':
    sys.exit(run(sys.argv[1] if len(sys.argv) > 1 else 'verify'))
