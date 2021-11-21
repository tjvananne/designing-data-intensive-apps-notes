

# bytes objects in Python can only contain ASCII literal characters

mychar = '大'
print(mychar.encode())              # b'\xe5\xa4\xa7'

print(len(mychar))                  # 1 (in text mode, it is only one character)
print(len(mychar.encode()))         # 3 (in encoded UTF-8, it is three bytes)
print(len(mychar.encode('utf-16'))) # 4 (when encoded in UTF-16, requires four bytes)

with open("chinese_character.txt", "wb") as f:
    f.write(mychar.encode())   # writes the character to file and correctly reports 3 bytes written

