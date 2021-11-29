
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

    def test_simple_write_and_read(self):
        
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


    def test_standard_segment_rotation(self):
        """
        When we get to a certain size file, does the BitCask object move on
        to the next file?
        """

        # set up and make sure it's cleared
        dir_path = "test_two"
        bc = BitCask(directory_path=dir_path)
        bc_delete(bc, dir_path)

        # set back up and execute test
        bc = BitCask(directory_path=dir_path)
        
        # while os.stat(bc.current_file_fullpath).st_size < (ONE_MB_IN_BYTES * 3):
        while bc.current_file_number < 3:

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

        self.assertIn(dir_path + '/segment_0000000', bc.inactive_segments)
        self.assertIn(dir_path + '/segment_0000001', bc.inactive_segments)

        # clean up
        bc_delete(bc, dir_path)

        print('done.')


    def test_segment_rotation_reset(self):
        """
        Eventually, we'll run into the situation where we reach the segment
        file of "segment_9999999", in which case, we should roll this over
        to "segment_0000000". I want to make sure that happens gracefully.

        TODO: come back and give this some more thought. Right now, I'm just
        looking for the "max" filename that matches the segment file pattern.
        Maybe instead, I should look for the most recently modified file
        since filename doesn't necessarily indicate which file may be the
        most recent.
        """

        # set up and make sure it's cleared
        dir_path = "test_three"
        bc = BitCask(directory_path=dir_path)
        bc_delete(bc, dir_path)

        # start at a file segment number that is already near it's maximum
        os.makedirs(dir_path)
        with open(dir_path + '/segment_9999998', 'wb') as f:
            f.write(b'')

        # set back up and execute test
        bc = BitCask(directory_path=dir_path)
        
        while bc.current_file_number != 3:

            bc.put(b'key1', b'value1 first value')
            bc.put(b'key2', b'value2 first value')
        
        all_files = os.listdir(bc.directory_path)
        all_segment_files = []
        for file in all_files:
            if re.search(bc._FILE_SEG_PATTERN, file):
                all_segment_files.append(re.search(bc._FILE_SEG_PATTERN, file).group(0))

        self.assertIn('segment_9999998', all_segment_files)
        self.assertIn('segment_9999999', all_segment_files)
        self.assertIn('segment_0000000', all_segment_files)
        self.assertIn('segment_0000001', all_segment_files)

        self.assertIn(dir_path + '/segment_9999998', bc.inactive_segments)
        self.assertIn(dir_path + '/segment_9999999', bc.inactive_segments)
        self.assertIn(dir_path + '/segment_0000000', bc.inactive_segments)
        self.assertIn(dir_path + '/segment_0000001', bc.inactive_segments)

        # clean up
        bc_delete(bc, dir_path)

        print('done.')






