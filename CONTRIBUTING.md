# Contributing to MongoSchematic

Thank you for your interest in contributing to MongoSchematic! We welcome contributions from the community.

## Getting Started

1.  **Fork** the repository.
2.  **Clone** your fork:
    ```bash
    git clone https://github.com/your-username/mongo-schematic.git
    cd mongo-schematic
    ```
3.  **Install** dependencies:
    ```bash
    pip install -e ".[dev]"
    ```
4.  **Install** Git hooks:
    ```bash
    mschema hook install
    ```

## Development Workflow

1.  Create a new branch: `git checkout -b feature/my-feature`.
2.  Make your changes.
3.  Ensure tests pass:
    ```bash
    pytest
    ```
4.  Commit your changes (our pre-commit hooks will verify drift).
5.  Push and open a Pull Request!

## Reporting Issues

Please check existing issues before opening a new one. Include:
- Steps to reproduce
- Expected vs actual behavior
- `mschema` version

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
