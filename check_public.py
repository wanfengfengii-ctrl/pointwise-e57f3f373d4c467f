"""Optional tiny independent examples; no production helper computes expectations."""
import collections
import json
import sys
from joiner import join
from spill import Metrics

def record(ident, key):
    return {'id': ident, 'key': key, 'payload': ident + ':' + 'q' * 65}

def run(name):
    if name == 'duplicates':
        left = [record('l1', 'k'), record('l2', 'k')]
        right = [record('r1', 'k'), record('r2', 'k'), record('r3', 'k')]
        pairs = [(a['id'], b['id']) for a in left for b in right]
        budget = 8192
    elif name == 'nulls':
        left, right = [record('l', None)], [record('r', None)]
        pairs, budget = [('l', None), (None, 'r')], 512
    elif name == 'spill':
        # These two keys share bucket zero; neither key is absent from the other side.
        left = [record('la' + str(i), '1') for i in range(6)]
        left += [record('lb' + str(i), '2') for i in range(5)]
        right = [record('rb' + str(i), '2') for i in range(5)]
        right += [record('ra' + str(i), '1') for i in range(6)]
        pairs = [('la' + str(i), 'ra' + str(j)) for i in range(6) for j in range(6)]
        pairs += [('lb' + str(i), 'rb' + str(j)) for i in range(5) for j in range(5)]
        budget = 1024
    else:
        raise ValueError('Unknown public case: ' + name)
    lp = {r['id']: r['payload'] for r in left}
    rp = {r['id']: r['payload'] for r in right}
    expected = [[a, None if a is None else lp[a],
                 b, None if b is None else rp[b]] for a, b in pairs]
    metrics = Metrics()
    actual = list(join(iter(left), iter(right), budget, metrics=metrics))
    canonical = lambda rows: collections.Counter(json.dumps(x, ensure_ascii=False) for x in rows)
    if canonical(actual) != canonical(expected) or metrics.live or metrics.peak > budget:
        print('PUBLIC_' + name.upper() + '_MISMATCH',
              json.dumps({'expected_rows': len(expected), 'actual_rows': len(actual),
                          'peak': metrics.peak, 'live': metrics.live}))
        return 1
    print('PUBLIC_' + name.upper() + '_OK',
          json.dumps({'rows': len(actual), 'peak': metrics.peak}))
    return 0

if __name__ == '__main__':
    sys.exit(run(sys.argv[1]))
