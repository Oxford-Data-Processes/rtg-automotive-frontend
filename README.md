# rtg-automotive-frontend

Checks:

pre-commit run --all-files

mypy {path_to_file_or_directory} --explicit-package-bases

Commands:

uvicorn app.api.mock:app --host 0.0.0.0 --port 8000 --reload
