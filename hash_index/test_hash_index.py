
import unittest
import hash_index
import os

class TestHashIndex(unittest.TestCase):
    """
    Bare-bones minimal tests to make sure the hash index works as expected.
    """

    def test_basic_read_write(self):
        """
        1. Write to key "Nelson", then "Ari", then "Nelson" again.
        2. Read from the "Nelson" key
        3. Assert that what we read is the latest thing we wrote to the file
        """
    
        file_path = "basic_read_write.txt"
        
        if os.path.exists(file_path):
            os.remove(file_path)
        
        hi = hash_index.HashIndex(file_path)
        hi.write("Nelson", b"is such a good boy.")
        hi.write("Ari", b"is the best")
        hi.write("Nelson", b"is most annoying at night")
        self.assertEqual(b"is most annoying at night", hi.read("Nelson"))

        os.remove(file_path)
    

    def test_read_from_existing_file(self):
        """
        1. Write to key "Nelson", then "Ari", then "Nelson" again.
        2. Create a new hash_index object at the same file path
        3. Read from the latest "Nelson" key
        4. Assert that it's what it should be
        """
    
        file_path = "basic_read_write.txt"
        
        if os.path.exists(file_path):
            os.remove(file_path)
        
        hi = hash_index.HashIndex(file_path)
        hi.write("Nelson", b"is such a good boy.")
        hi.write("Ari", b"is the best")
        hi.write("Nelson", b"is most annoying at night")

        hi2 = hash_index.HashIndex(file_path)
        self.assertEqual(b"is most annoying at night", hi2.read("Nelson"))

        os.remove(file_path)


    def test_exception_on_large_key_01(self):
        """
        """
    
        file_path = "basic_read_write.txt"
        
        if os.path.exists(file_path):
            os.remove(file_path)
        
        hi = hash_index.HashIndex(file_path)
        hi.write("Nelson", b"is such a good boy.")
        with self.assertRaises(Exception):
            too_long_of_a_key = 'a' * 150
            hi.write(too_long_of_a_key, b"We shouldn't be able to write these bytes.")

        os.remove(file_path)


    def test_read_from_key_that_does_not_exist(self):
        """
        1. Write to key "Nelson", then "Ari", then "Nelson" again.
        2. Read from the "Taylor" key
        3. Assert that method returns empty byte string and a warning message
        """
    
        file_path = "basic_read_write.txt"
        
        if os.path.exists(file_path):
            os.remove(file_path)
        
        hi = hash_index.HashIndex(file_path)
        hi.write("Nelson", b"is such a good boy.")
        hi.write("Ari", b"is the best")
        hi.write("Nelson", b"is most annoying at night")
        self.assertEqual(b"", hi.read("Taylor"))

        os.remove(file_path)


    def test_writing_and_reading_non_ascii_data(self):
        """
        1. Write to key "Nelson", then "Ari", then "Nelson" again.
        2. Read from the "Nelson" key
        3. Assert that what we read is the latest thing we wrote to the file
        """
    
        file_path = "basic_read_write.txt"
        
        if os.path.exists(file_path):
            os.remove(file_path)
        
        hi = hash_index.HashIndex(file_path)
        hi.write("Nelson", "大大大".encode())
        hi.write("Ari", "大大大".encode())
        hi.write("Nelson", "木山木".encode())
        self.assertEqual("木山木".encode(), hi.read("Nelson"))

        os.remove(file_path)


