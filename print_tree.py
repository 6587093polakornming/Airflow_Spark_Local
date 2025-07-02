from pathlib import Path

def print_tree(path=".", level=2, prefix="", file=None):
    base = Path(path)
    if level < 0:
        return
    entries = sorted(base.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
    for i, entry in enumerate(entries):
        connector = "└── " if i == len(entries) - 1 else "├── "
        line = f"{prefix}{connector}{entry.name}\n"
        if file:
            file.write(line)
        else:
            print(line, end="")
        if entry.is_dir():
            extension = "    " if i == len(entries) - 1 else "│   "
            print_tree(entry, level - 1, prefix + extension, file)

if __name__ == "__main__":
    with open("project_tree.txt", "w", encoding="utf-8") as f:
        print_tree(".", level=2, file=f)
    print("✅ Project structure saved to 'project_tree.txt'")