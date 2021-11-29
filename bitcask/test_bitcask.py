
import unittest
from bitcask import BitCask
import os
import glob
import re

ONE_MB_IN_BYTES = int(2 ** 20)

def bc_delete(bc: BitCask, dir_path: str):
    """
    Cleans up after a test has been run.
    """
    file_pattern = dir_path + '/' + bc._FILE_SEG_ID_PREFIX + '*'
    files_this_test = glob.glob(pathname=file_pattern)
    for file in files_this_test:
    
        try: 
            os.remove(file)
        except FileNotFoundError:
            pass
    
    try:
        os.removedirs(dir_path)
    except FileNotFoundError:
        pass


class TestBitCask(unittest.TestCase):

    def test_one(self):
        
        dir_path = "test_one"
        bc = BitCask(directory_path=dir_path)
        bc.put(b'key1', b'value1 first value')
        bc.put(b'key2', b'value2 first value')
        byte_message = b'value1 second value. This should be the one printed out.'
        bc.put(b'key1', byte_message)

        retrieved_value = bc.get(b'key1')
        self.assertEqual(retrieved_value, byte_message)

        bc_delete(bc, dir_path)        
        return


    def test_two(self):
        """
        Thoughts... I want to try some TDD on changing the active file once
        a certain file size threshold has been hit.
        """

        # set up and make sure it's cleared
        dir_path = "test_two"
        bc = BitCask(directory_path=dir_path)
        bc_delete(bc, dir_path)

        # set back up and execute test
        bc = BitCask(directory_path=dir_path)
        
        while os.stat(bc.current_file_fullpath).st_size < (ONE_MB_IN_BYTES * 3):

            bc.put(b'key1', b'value1 first value')
            bc.put(b'key2', b'value2 first value')
        
        all_files = os.listdir(bc.directory_path)
        all_segment_files = []
        for file in all_files:
            if re.search(bc._FILE_SEG_PATTERN, file):
                all_segment_files.append(re.search(bc._FILE_SEG_PATTERN, file).group(0))

        self.assertIn('segment_0000000', all_segment_files)
        self.assertIn('segment_0000001', all_segment_files)
        self.assertIn('segment_0000002', all_segment_files)

        self.assertIn('segment_0000000', bc.inactive_segments)
        self.assertIn('segment_0000001', bc.inactive_segments)


        print('done.')
