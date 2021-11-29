

import math
import sys

def get_num_bytes_of_int(hashmod_key: int) -> int:
    """
    Given an integer, tell me the minimum number of
    bytes required to represent this integer.
    """

    min_num_bytes = math.ceil(hashmod_key.bit_length() / 8)
    return len(hashmod_key.to_bytes(length = min_num_bytes, byteorder=sys.byteorder))


bytestring = b''
last_num_bytes_required = 0
while True:
    
    bytestring += b'a'
    len_bytestring = len(bytestring)
    num_bytes_required = get_num_bytes_of_int(len_bytestring)
    
    if num_bytes_required != last_num_bytes_required:
        print(f"Byte string of {len_bytestring} length requires {num_bytes_required} bytes.")
        last_num_bytes_required = num_bytes_required

    # setting a boundary / break point for my experiment
    if len_bytestring > 1e5:
        break

