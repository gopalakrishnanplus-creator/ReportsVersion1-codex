import os

# Extensions to include
ALLOWED_EXTENSIONS = {".py", ".html", ".css", ".js"}

# Directories to ignore
EXCLUDED_DIRS = {
    "venv",
    ".git",
    "__pycache__",
    "node_modules",
    "migrations",
    ".vscode"
}

OUTPUT_FILE = "django_project_export.txt"


def should_include_file(filename):
    return os.path.splitext(filename)[1] in ALLOWED_EXTENSIONS


def export_files(root_dir):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as output:
        for root, dirs, files in os.walk(root_dir):
            # Remove excluded directories from traversal
            dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]

            for file in files:
                if should_include_file(file):
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, root_dir)

                    output.write(f"\n{'=' * 80}\n")
                    output.write(f"FILE: {relative_path}\n")
                    output.write(f"{'=' * 80}\n\n")

                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            output.write(f.read())
                    except Exception as e:
                        output.write(f"[Could not read file: {e}]\n")


if __name__ == "__main__":
    project_root = os.getcwd()  # Run from project root
    export_files(project_root)
    print(f"Export complete! Files saved to '{OUTPUT_FILE}'")
