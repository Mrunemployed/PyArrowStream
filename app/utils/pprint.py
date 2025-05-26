import re

def pprint(text:str,color:str=None,*args):
    text = str(text)
    misc = " ".join(args)
    colors_dict = {
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "magenta": "\033[35m",
        "cyan": "\033[36m",
        "white": "\033[37m",
        "reset": "\033[0m"
    }
    exp = re.compile(r"\[.*\]")
    matched = exp.match(text)
    if matched:
        h_start = matched.start()
        h_end = matched.end()
        colors = colors_dict.get(color) if color else colors_dict.get('cyan')
        print(f"{colors}{text[h_start:h_end]}{colors_dict['reset']}{text[h_end:]} {misc or ''}")
        return
    colors = colors_dict.get('magenta')
    print(f"{colors}{text} {misc or ''}{colors_dict['reset']}")