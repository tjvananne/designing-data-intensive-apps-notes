
from datetime import datetime
import os
import re
from typing import Optional
import math
import sys


# For Reference:
# int.to_bytes() will default to "signed=False"
# Byte string of 1 length requires 1 bytes.
# Byte string of 256 length requires 2 bytes.
# Byte string of 65536 length requires 3 bytes.
# Keys will be a fixed size of 2 bytes, values will be a fixed size of 3 bytes
# timestamps will be 26 bytes 


class BitCask():
    """
    second attempt at developing a log-based on-disk hash index with in-memory hashed keys.

    This object only reads/writes bytes. It is up to the end user to decode/encode appropriately.

    Args:
    - directory_path:   this path will be created upon initialization of this 
                        object. this is where all segment data files will be
                        stored and written to (before and after compaction).
    - write:    boolean to represent whether this instance of the object has
                the ability to write. Default is to create objects that can 
                only read. "We're all adults here" - so don't create multiple
                objects with write access in the same directory. 
    """
    
    def __init__(self, directory_path: Optional[str] = None, write: bool = True,
                hash_table_size: int = int(1e7)):
        """
        Opens (or creates) a directory for the BitCask object to read from (and optionally
        to write to).)
        """

        # TODO: set up like a file_number attribute, then build the current_file name off of that
        # that way, we can simply increment it when we need to change files.
        # this will also require me to regex for existing file patterns

        self._hash_table_size = hash_table_size
        self.writable = write

        # for determining key size and byte IO
        self.byteorder = sys.byteorder

        # NOTE: for simplicity, I'll always assume these values even when
        # reading from an existing data dir.
        self._FILE_SEG_ID_PREFIX = "segment_"
        self._FILE_SEG_ID_DIGITS = 7
        self._FILE_SEG_PATTERN = '^' + self._FILE_SEG_ID_PREFIX + '[0-9]{' + str(self._FILE_SEG_ID_DIGITS) + '}$'
        self._FILE_SEG_BYTE_THRESHOLD = 2 ** 20
        self._KEYSIZE_BYTES = 2
        self._VALUESIZE_BYTES = 3

        self.inactive_segments = []

        # determine the data directory path for this instance of bitcask
        if not directory_path:
            directory_path = "default_data_dir"
        self.directory_path = directory_path
        try:
            os.makedirs(self.directory_path)
        except FileExistsError:
            print("Directory already exists, will search for segment files in existing data directory...")

        # identify the current active segment file where new writes will take place
        # existing_data_files = [f for f in os.listdir(self.directory_path) if f.startswith(self._FILE_SEG_ID_PREFIX)]
        existing_data_files = [f for f in os.listdir(self.directory_path) if re.search(self._FILE_SEG_PATTERN, f)]
        if existing_data_files:
            self.current_file = max(existing_data_files)
            self.current_file_number = self.current_file # TODO: BOOKMARK, I was right here, setting up file_numbers for easier addition.
            # TODO: I should also probably have a private method for formatting the current_file_number to string with leading zeros
        else:
            self.current_file = self._filename_format(0)
            print("No segment files existed, starting from segment file zero...")
            with open(self.current_file_fullpath, 'wb') as f:
                f.write(b'')
            self.current_file_number = 0

        # initialize the keydir (in-memory hashed key structure)
        # This will be a list of dicts where each dict contains file_id, value_size, value_pos, timestamp
        self.key_table_size = hash_table_size
        self.keydir = [-1] * self.key_table_size


    # reminds me of a view in SQL RDBMS. Looks like an object property, 
    # but it's really a wrapper around some logic that executes
    @property
    def current_file_fullpath(self):
        return self.directory_path + '/' + self.current_file


    def _lpad_zeros(self, segment_number: int) -> str:
        return "{:0" + str(self._FILE_SEG_ID_DIGITS) + "d}".format(segment_number)

    def _filename_format(self, segment_number: int):
        return (self._FILE_SEG_ID_PREFIX + ("{:0" + str(self._FILE_SEG_ID_DIGITS) + "d}").format(segment_number))


    def put(self, key: bytes, value: bytes) -> None:
        """
        Writing data consist of the following steps that BOTH INVOLVE SIDE EFFECTS:
        1. write the data to disk according to the current, active file segment:
            - timestamp  (fixed 26 bytes)
            - key_size   (fixed 2 bytes)
            - value_size (fixed 3 bytes)
            - key        (variable size)
            - value      (variable size)
        2. write or update the in-memory keydir list of dictionaries
            - file_id
            - value_sz
            - value_pos
            - tstamp
        """

        if not self.writable:
            raise Exception("This instance of BitCask is not writable")

        # make sure key and value are within the size limits for our data structure
        if self._get_num_bytes_of_int(len(key)) > self._KEYSIZE_BYTES:
            raise Exception("Key is too large to be stored in this structure.")
        
        if self._get_num_bytes_of_int(len(value)) > self._VALUESIZE_BYTES:
            raise Exception("Value is too large to be stored in this structure.")

        _ts = str(datetime.utcnow())

        # append bytes to our current log segment file
        with open(self.directory_path + '/' + self.current_file, "ab") as f:
            # write fixed size elements
            f.write(_ts.encode('utf-8'))
            f.write(len(key).to_bytes(length=self._KEYSIZE_BYTES, byteorder=sys.byteorder))
            f.write(len(value).to_bytes(length=self._VALUESIZE_BYTES, byteorder=sys.byteorder))
            # write variable sized elements
            f.write(key)
            value_position = f.tell()  # capture starting position of value
            f.write(value)
        
        # update in-memory keydir
        key_dict = {
            'file_id': self.current_file,
            'value_size': len(value),
            'value_position': value_position,
            'timestampe': _ts
        }
        hashmod_int_key = self._hashmod_this_key(key)
        self.keydir[hashmod_int_key] = key_dict

        # TODO: check file size (value_position will work)
        if value_position > self._FILE_SEG_BYTE_THRESHOLD:
            self._change_active_file()
        return

    def _change_active_file(self):
        pass

    def get(self, key: bytes) -> bytes:
        """
        Query the log-structure hash index by key.
        """
        # key_dict = {
        #     'file_id': self.current_file,
        #     'value_size': len(value),
        #     'value_position': value_position,
        #     'timestampe': _ts
        # }


        key_dir_record = self.keydir[self._hashmod_this_key(key)]
        with open(self.directory_path + '/' + key_dir_record['file_id'], 'rb') as f:

            f.seek(key_dir_record['value_position'])
            value = f.read(key_dir_record['value_size'])
        
        return value


    def _get_num_bytes_of_int(self, this_int: int) -> int:
        """
        Given an integer, tell me the minimum number of
        bytes required to represent this integer.

        This will commonly be used as such:
            _get_num_bytes_of_int(len(some_bytes))
        """
        return math.ceil(this_int.bit_length() / 8)


    def _hashmod_this_key(self, key_bytes: bytes) -> int:
        return int(hash(key_bytes) % self._hash_table_size)



if __name__ == "__main__":

    bc = BitCask()
    print("file seg pattern: ", bc._FILE_SEG_PATTERN)
    bc.put(b'nelson', b'is always hyper.')
    bc.put(b'taylor', b'wants some food.')
    print(bc.keydir[bc._hashmod_this_key(b'nelson')])
    print(bc.keydir[bc._hashmod_this_key(b'taylor')])
    print("\ntesting a read:")
    bc.put(b'nelson', b'has a new message for me!')
    print(bc.get(b'nelson'))
    print(bc.get(b'taylor'))


