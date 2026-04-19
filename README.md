# astock-lifespan-alpha

`astock-lifespan-alpha` is the reconstructed successor to `lifespan-0.01`.

The formal mainline is:

```text
data -> malf -> alpha -> position -> portfolio_plan -> trade -> system
```

This repository deliberately excludes `structure` and `filter` from the formal system architecture.

The first-edition implementation stack is:

- Python
- DuckDB
- Arrow

