# Contributing

`mt` is an early-stage C++20 micro-transaction core library. Keep changes small,
focused, and covered by tests.

## Development Checks

Run the full check before submitting changes:

```sh
make check
```

Format C++ sources with:

```sh
make format
```

Generate PlantUML documentation images with:

```sh
make docs-png
```

## Code Guidelines

- Preserve C++20 compatibility.
- Keep optional backend dependencies out of the core build.
- Prefer extending tests with behavior changes.
- Keep generated files under `build/` out of commits.
- Keep application schemas in user projects. This repository should only contain example
  schemas used for documentation and tests.
- Prefer narrow public headers over expanding `mt/core.hpp` when implementing backend
  modules.

## Backend Contributions

Backend implementations should follow:

- `docs/backend_contract.md`
- `docs/backend_implementation.md`

Production backends must keep their external database dependencies optional and isolated
from the default core build.
