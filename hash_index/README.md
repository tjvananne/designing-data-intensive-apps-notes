
# Creating a Hash Index Structure

Page 72 of Designing Data-Intensive Applications talks about a simple Hash Index. I wanted to try and implement that in Python.

## Retrospection

Compaction seems really annoying on a log-based append-only data file on disk. I read ahead to the sorted string tables (SSTables), and the compaction seems so much easier there, that I'm finding it hard to even bring myself to write some code for compaction on this log-based hash index.

# Notes


**Note:** I do not care at all about being pythonic in this experiment. I'm going to write a lot of disk IO that is character-by-character. I know this isn't pythonic and not efficient. **I'm optimizing for learning** right now. What's most important to me is how the structure works. How to do compaction while also serving reads (and writes?). In retrospect, because I'm doing so many character-by-character reads and writes, I probably should have picked a different language like C or Java, but I'm most comfortable in Python and am only interested in the concepts of a hash index anyway.

Pros and cons of this structure and when it's useful:
- You want key-value pair storage
- You have few enough keys that they all fit in memory
- This works well in write-heavy situations with relatively few keys (see point of keys fitting in memory above)
- Example use case, tracking the hits on website URLs

Assumptions about my structure:
* a complete record on disk will look like `natural_key:data\n`
    - `natural_key` is the key the user wants to use to store this data as
    - `data` is the data they want to store
    - all records end with a newline character
    - the `natural_key:` and newline character portion of the record are ommitted upon read, only the `data` is returned
* the object in memory which will serve as my hash map will be a python list object
    - I'll initialize a pretty big list (1e7 default) to avoid collisions, but I won't explicitly handle collisions
    - If you hash the natural key and modulo that result by the in-memory hashmap list object size, you'll receive the index in the list for that specific natural key
    - The value within the list object will be the cursor point within the file on disk to seek to to begin reading (until you hit a newline). This is the data currently associated with the natural_key
* the natural_key may only be up to 100 characters and must be at least one character that is not a `:` 
* the natural_key must not contain a `:` or a `\n` (this is really only inforced upon writes, but it should be specifically called out since you can rebuild the in-memory hash map of the file based on a pre-existing data file on disk)
* Natural keys will be provided as `str`, but data being written to disk must be given as `bytes`


## Next Steps

* I'd like to write compaction logic
* Then see if I can run my compaction at the same time as many writes
    - Do I have to select a certain point in time to begin my compaction and work backwards from the end of the file/log?
    - Will that even work while writes are happening simultaneously?
    - These points in the book are critical:
        - "*...how do we avoid eventually running out of disk space? A good solution is to break the log into segments of a certain size by closing a segment file when it reaches a certain size, and making subsequent writes to a new segment file. We can then perform compaction on these segments...*"
        - "*...we can also merge several segments together at the same time as performing the compaction... Segments are never modified after they have been written, so the merged segment is written to a new file.*"
            - ok, this is confusing to me for a few reasons. Now, not only do our keys have to point to a specific place in a file to seek to, but they also have to tell us which specific file we should even be seeking in at all. There's really only about 4 pages in the book on this structure, so I may have to read into some of the resources he's referencing.
* Don't just read until a newline character. Instead, save the data size in bytes to read, this will be much more efficient.
* key-value pair should actually be a dict of dicts, or maybe list of dicts
    - this way, I can store the key size, data size, and even file_id (bitcask uses one active file and compacts only the inactive segments)

## The original white paper:

https://riak.com/assets/bitcask-intro.pdf

- This is a reference in the Designing Data-Intensive Applications book related to bitcask.
- Are hint files only for disaster recovery? Or is there a reason for them?
    - Do you do a check for if the key exists in active segment? Then if not, you can reference the latest hint file?


