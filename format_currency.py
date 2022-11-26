def to_satoshi(value, decimals=8):
    value = float(value)
    decimals = int(decimals)
    value = int(value * 10 ** decimals)
    return value


def from_satoshi(value, decimals=8, precision=None):
    value = int(value)
    decimals = int(decimals)
    value = value * 10 ** (-decimals)
    if precision is not None:
        value = round(value, precision)
    return value