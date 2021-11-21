
# Notes and references:
# * https://stackoverflow.com/a/58925279/3586093 (this is a really cool diagram)
# file.tell() works better in bytes mode than text mode...
# opening a file in append mode will write it if it doesn't exist, nice!

from typing import Optional, List
import os



class HashIndex():


    def __init__(self, file_path: str, hash_table_size: Optional[int] = None):

        self.file_path = file_path

        if not hash_table_size:
            hash_table_size = int(1e7)
        self.hash_table_size = hash_table_size

        # this is my in-memory, key-value hash table
        # (yes, I get it that I'm essentially creating a worse version of a dict, this is for learning purposes)
        self.kv = [-1] * self.hash_table_size

        if os.path.exists(file_path):
            if os.path.getsize(file_path):
                with open(self.file_path, "rb") as f:
                    self.kv = self._build_kv_from_disk(filehandle=f, kv=self.kv)


    def _build_kv_from_disk(self, filehandle, kv: List[int]) -> List[int]:

        # read until colon (make sure it isn't the first character)
        # Colon can't be first character after a newline
        # All I really care about here would be:
        # -     

        last_char = b''
        natural_key_capture = True
        this_natural_key = b''
        file_position_of_this_key = 0
        natural_key_char_count = 0

        while True:

            # I feel like I'm writing C code in Python, this is odd, but fun.
            c = filehandle.read(1)

            # EOF
            if c == b'':
                break

            # check for the end of the natural key
            if c == b':' and natural_key_capture:
                if natural_key_char_count == 0:
                    raise Error("Natural Key cannot start with ':'.")
                natural_key_capture = False
                _hash_idx = self._hashmod_the_key(this_natural_key.decode(), self.kv)
                kv[_hash_idx] = file_position_of_this_key
                this_natural_key = b''
                natural_key_char_count = 0

            # capturing characters that make up the natural key
            if natural_key_capture:
                if natural_key_char_count > 100:
                    raise Error("Natural key character count has exceeded limit of 100.")
                this_natural_key += c
                natural_key_char_count += 1

            # when reading bytes, a newline character is a single byte
            if (last_char == b'\\' and c == b'n') or c == b'\n':
                if natural_key_capture:
                    raise Error("Cannot have a newline char in natural key.")
                natural_key_capture = True
                file_position_of_this_key = filehandle.tell()

            last_char = c

        return kv


    def _read_file_til_newline(self, filehandle) -> bytes:
        """
        The point here is, filehandle is a file object (in read bytes mode) that 
        we've already executed a seek-to method on for where we want to start
        reading. Then we execute this 
        """
        
        result_bytes = b''
        last_c = b''        
        
        while True:

            c = filehandle.read(1)
            
            if not c:
                return result_bytes

            if c == b'\n':
                return result_bytes

            if (last_c == b'\\' and c == b'n'):
                return result_bytes[:-1]

            last_c = c
            result_bytes += c


    def _hashmod_the_key(self, natural_key: str, kv: List[int]) -> int:
        return hash(natural_key) % len(kv)# self.hash_table_size


    def write(self, natural_key: str, data: bytes) -> int:
        """
        Writes to the append-only Hash index structure on disk and 
        keeps a key-value in-memory hash table updated.         
        """

        if ":" in natural_key:
            raise Exception("Cannot have a ':' in your natural key.")
        
        if len(natural_key) > 100:
            raise Exception("Natural key cannot exceed 100 characters.")

        hashed_key = self._hashmod_the_key(natural_key, self.kv)
        data = natural_key.encode() + b":" + data + b"\n"

        # will create file if doesn't exist
        # when in append mode, file cursor starts at the end of the file
        with open(self.file_path, "ab") as f:

            # will be the value for this key
            current_position = f.tell()
            count_of_bytes_written = f.write(data)

        # update value for key
        self.kv[hashed_key] = current_position
        
        return count_of_bytes_written


    def read(self, natural_key: str) -> bytes:

        len_natural_key = len(natural_key)
        hashed_key = self._hashmod_the_key(natural_key, self.kv)

        with open(self.file_path, "rb") as f:
            
            # seek to just the data we want
            seek_to = self.kv[hashed_key]
            if seek_to < 0:
                print(f"No value for {natural_key} key yet. Returning empty bytes.")
                return b''

            f.seek(seek_to)

            result_bytes = self._read_file_til_newline(f)
            result_bytes = result_bytes[(len_natural_key + 1):]
                
        return result_bytes


if __name__ == "__main__":

    print("Stress testing the hash_index:")
    print("Probably a good idea to make sure the tests are all passing:")
    print("python -m unittest test_hash_index.py")
    import random

    keys = [str(x) for x in range(1000, 3000)]
    base_message = "This is a message we want to store on disk "
    fp = 'this.db'

    if os.path.exists(fp):
        os.remove(fp)

    hi = HashIndex(fp)
    for i in range(0, int(1e7)):

        if i % int(1e4) == 0:
            print(i)

        # write on each iteration
        this_message = base_message + str(i)
        this_natural_key = random.choice(keys)
        hi.write(this_natural_key, this_message.encode())

        # # toss in a read every once in a while
        # if i % 13 == 0:
        #     read_key = random.choice(keys)

