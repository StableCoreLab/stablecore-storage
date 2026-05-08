import os

TEXT_EXT = {".cpp", ".h", ".hpp", ".c", ".txt", ".cmake", ".json"}

def normalize_eol(root):
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            ext = os.path.splitext(f)[1].lower()
            if ext not in TEXT_EXT:
                continue

            path = os.path.join(dirpath, f)

            with open(path, "rb") as fp:
                content = fp.read()

            # ͳһΪ LF
            content = content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
            #content = content.replace(b"\n", b"\r\n")

            with open(path, "wb") as fp:
                fp.write(content)

normalize_eol(".")