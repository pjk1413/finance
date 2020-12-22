

def clean(string):
    remove_chars = ['[', ']', '{', '}', '"']
    for char in remove_chars:
        string = string.replace(char, '')
    return string

def list_to_database(list):
    str = ""
    for item in list:
        str += item + "::"
    return str[0:len(str)-2]

def list_from_database(string):
    return string.split('::')
