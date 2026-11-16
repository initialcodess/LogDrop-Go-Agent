"""A small OTLP trace request reader for the proof (no protobuf package): span names, IDs, attributes, resource."""

import struct


def _varint(data, i):
    shift = result = 0
    while True:
        b = data[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def fields(data):
    i = 0
    while i < len(data):
        key, i = _varint(data, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, i = _varint(data, i)
        elif wire == 1:
            value, i = data[i:i + 8], i + 8
        elif wire == 2:
            length, i = _varint(data, i)
            value, i = data[i:i + length], i + length
        elif wire == 5:
            value, i = data[i:i + 4], i + 4
        else:
            raise ValueError("wire type")
        yield field, value


def _any(data):
    for field, value in fields(data):
        if field == 1:
            return value.decode()
        if field == 2:
            return bool(value)
        if field == 3:
            return value - (1 << 64) if value >= 1 << 63 else value
        if field == 4:
            return struct.unpack("<d", value)[0]
    return None


def _attributes(items):
    out = {}
    for item in items:
        parts = dict(fields(item))
        out[parts[1].decode()] = _any(parts.get(2, b""))
    return out


def decode(body):
    spans = []
    for _, resource_spans in fields(body):
        resource = {}
        for field, value in fields(resource_spans):
            if field == 1:
                resource = _attributes([v for f, v in fields(value) if f == 1])
            elif field == 2:
                for f2, span in fields(value):
                    if f2 != 2:
                        continue
                    parts = list(fields(span))
                    get = lambda n: next((v for f, v in parts if f == n), b"")
                    spans.append({"name": get(5).decode(), "trace_id": get(1).hex(), "span_id": get(2).hex(),
                                  "parent_span_id": get(4).hex(), "kind": get(6) or 0,
                                  "attributes": _attributes([v for f, v in parts if f == 9]), "resource": resource})
    return spans
