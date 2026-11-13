from joiner import bucket

def row(ident, key, pad=0):
    return {'id': ident, 'key': key, 'payload': ident + ':' + 'x' * pad}

def normal():
    return ([row('l1', 'a'), row('l2', 'a'), row('l3', None), row('l4', 'z')],
            [row('r1', 'a'), row('r2', 'b'), row('r3', None)])

def incident():
    # Two real business keys sharing the first partition, in opposite block order.
    keys = [str(i) for i in range(200) if bucket(str(i), 0, 4) == 0][:2]
    a, b = keys
    return ([row('la' + str(i), a, 65) for i in range(6)] +
            [row('lb' + str(i), b, 65) for i in range(5)],
            [row('rb' + str(i), b, 65) for i in range(5)] +
            [row('ra' + str(i), a, 65) for i in range(6)])
