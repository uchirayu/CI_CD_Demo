def add(a: int, b: int) -> int:
    """Return the sum of two integers."""
    return a + b


def greet(name: str) -> str:
    """Return a greeting message."""
    return f"Hello, {name}!"


if __name__ == "__main__":
   
    x, y = 2, 3
    print(f"{x} + {y} = {add(x, y)}")
    print(greet("GitHub Actions"))