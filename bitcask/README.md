

# ok, this will be the hash index I build after reading the BitCask PDF

## The original white paper:

https://riak.com/assets/bitcask-intro.pdf

- This is a reference in the Designing Data-Intensive Applications book related to bitcask.
- Are hint files only for disaster recovery? Or is there a reason for them?
    - Do you do a check for if the key exists in active segment? Then if not, you can reference the latest hint file?

## Explicit ommissions

- I will not be implementing any of the following (yet, at least):
    - `list_keys()`: I just don't care about this
    - `open()`: this will be handled by my `__init__` of the class itself. If I want to open a new dir for a BitCask object, I'll initialize a new one
    - `fold()`: it isn't clear to me what this actually is or does

## Unsure about...

- the keydir is the in-memory hashed key structure, I'll have to think about how to share this memory across multiple instances of my BitCask class? Or maybe I just won't? TBD on that one, my main focus is on the data structure itself (hashing, compaction/merging, get/put, etc)
- What are all the reasons why a timestamp in the log are important. Is it just for readers? Is it useful for compaction / merge? You'd think you can just read sequentially across files and that would be the proper order... maybe once we reach file_99999 and have to "rotate" our segment files (like you'd rotate logs), you then have to rely on time stamps for merge/compaction?

