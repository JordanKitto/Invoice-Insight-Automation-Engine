def step_header(title: str) -> None:
    """Print a consistent step header block."""
    print("\n" + "=" * 55)
    print(title)
    print("=" * 55)


def sub(message: str) -> None:
    """Print an indented sub-line within a step."""
    print(f"   {message}")
