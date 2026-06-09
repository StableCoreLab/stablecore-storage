import os

TEXT_EXT = {".cpp", ".h", ".hpp", ".c", ".txt", ".cmake", ".json"}


def normalize_eol(root):
    changed_count = 0
    skipped_count = 0

    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            ext = os.path.splitext(f)[1].lower()
            if ext not in TEXT_EXT:
                continue

            path = os.path.join(dirpath, f)

            try:
                with open(path, "rb") as fp:
                    content = fp.read()
            except OSError as exc:
                print(f"跳过无法读取的文件: {path} ({exc})")
                skipped_count += 1
                continue

            normalized = content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")

            if normalized == content:
                continue

            try:
                with open(path, "wb") as fp:
                    fp.write(normalized)
            except OSError as exc:
                print(f"跳过无法写入的文件: {path} ({exc})")
                skipped_count += 1
                continue

            changed_count += 1

    if changed_count or skipped_count:
        print(f"完成：已处理 {changed_count} 个文件，跳过 {skipped_count} 个文件。")


if __name__ == "__main__":
    normalize_eol(".")
