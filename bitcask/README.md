

# Bitcask Implementation in Python

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
- Should raw data files (such as `segment_0000001`) be named differently than the resulting files from compaction/merges? I mean, I'll have a hint file for any resulting compaction/merges. Also, compaction/merges must also look at resulting compaction/merge files in order to do a full compaction. How should that work? I guess the hint file helps with that? **This is probably the area that will consume the most time/thought of anything remaining that I'd like to build in this experiment**.


## Current biggest issues with my implementation

It sounds like the BitCask white paper talks about passing file handles around, whereas I'm passing file paths around and having to open/close the file for every write and read. This is probably the biggest departure from the white paper. I don't think it'd be a huge deal to open the active file only once and seek around to what I need... Not sure if I'll get to that or not. My main goal here is just to understand the hashing, log-based structure, and compaction/merge processes. I'm not actually looking to make this a production-grade data storage solution.

