import struct
import sys

# from twsapi / common.py

UNSET_INTEGER = 2 ** 31 - 1
UNSET_DOUBLE = sys.float_info.max

# from twsapi / comm.py

def make_msg(text) -> bytes:
    """ adds the length prefix """
    msg = struct.pack("!I%ds" % len(text), len(text), str.encode(text))
    return msg


def make_field(val) -> str:
    """ adds the NULL string terminator """
    if val is None:
        raise ValueError("Cannot send None to TWS")
    # bool type is encoded as int
    if type(val) is bool:
        val = int(val)
    field = str(val) + '\0'
    return field

def make_field_handle_empty(val) -> str:
    if val is None:
        raise ValueError("Cannot send None to TWS")
    if UNSET_INTEGER == val or UNSET_DOUBLE == val:
        val = ""
    return make_field(val)

# zmapi additions

DECODERS = {
    str: lambda x: x.decode(),
    int: lambda x: int(x),
    float: lambda x: float(x),
    bool: lambda x: bool(int(x)),
}

def decode(ftype, itr):
    b = next(itr)
    return DECODERS[ftype](b)



