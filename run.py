"""Read one reconciliation request, stream one result array per line."""
import json
import sys
from joiner import join
from spill import Metrics

def main():
    with open(sys.argv[1], encoding='utf-8') as source:
        request = json.load(source)
    metrics = Metrics()
    for result in join(iter(request['left']), iter(request['right']),
                       request['budget'], fanout=request.get('fanout', 4), metrics=metrics):
        print(json.dumps(result, ensure_ascii=False))
    print(json.dumps({'peak_payload': metrics.peak}), file=sys.stderr)

if __name__ == '__main__':
    main()
