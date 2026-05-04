# Contributing

## Dev environment

The repo uses [devenv](https://devenv.sh) with [uv](https://docs.astral.sh/uv/) and direnv. With direnv allowed, the shell auto-activates the environment. Otherwise:

```sh
devenv shell
uv sync --extra dev
```

Type-check with `pyright` (Nix-installed) or `uvx pyright`. Run tests with `uv run pytest`. Lint imports with `uv run lint-imports`.

## Releases

Cairns is alpha software released frequently, so the patch cadence is **automatic on every green push to `main`**. Minor and major bumps are explicit.

### Daily cadence (auto patch)

Every push to `main` that passes CI triggers `prepare-release` in `.github/workflows/ci.yml`:

1. `scripts/release_version.py print-next --level patch` returns the next version (latest `v*` tag + 1, unless `pyproject.toml` is already ahead — see "Manual minor/major" below).
2. The version is written into `pyproject.toml` and `uv.lock`.
3. The bot commits `Release vX.Y.Z [skip ci]` to `main`, tags `vX.Y.Z`, and pushes both.
4. The bot dispatches `release.yml` with `--ref vX.Y.Z -f tag=vX.Y.Z`. The publish job checks out the tag, re-runs tests, builds, publishes to PyPI via OIDC trusted publishing, and creates a GitHub release.

There is no `PYPI_TOKEN` secret. Publishing is gated by the `pypi` GitHub environment, which is restricted to `refs/tags/v*` deploys.

### Pausing or skipping releases

Three layered switches, in order of precedence:

| Mechanism | Effect | Where |
|---|---|---|
| `vars.AUTO_RELEASE = "false"` | Pauses all auto-bumps | Repo settings → Variables |
| `[no release]` in commit message | Skips one-off | Commit author |
| `[skip ci]` in commit message | Skips CI entirely | Commit author |

Tests still run on `[no release]`; they don't on `[skip ci]`.

### Manual minor / major / rc

The auto path always picks `patch`. To cut a different bump, edit `pyproject.toml` so the workspace version is **ahead of the latest `v*` tag**, commit, and push. `print-next` defers to the workspace version when it's ahead, so the next push tags whatever you wrote:

```sh
uv run scripts/release_version.py bump minor   # writes 0.3.0
git commit -am "chore: cut 0.3.0"
git push                                         # CI tags v0.3.0 and publishes
```

`bump` accepts `patch | minor | major | rc`. There is also a dispatch path: **Actions → Release → Run workflow → level=minor** runs the same logic on a runner.

### Re-publishing an existing tag

```sh
gh workflow run release.yml --ref vX.Y.Z -f tag=vX.Y.Z
```

The `--ref vX.Y.Z` matters: the `pypi` environment only allows tag refs. The workflow at that tag must be modern enough to accept the `tag` input (anything from `v0.2.5` onward).

### Architecture cheat sheet

| File | Job | Trigger |
|---|---|---|
| `.github/workflows/ci.yml` | `test` | PR + push to main |
| `.github/workflows/ci.yml` | `prepare-release` | push to main, gated by switches |
| `.github/workflows/release.yml` | `bump` | `workflow_dispatch` with `level`, `tag` empty |
| `.github/workflows/release.yml` | `publish` | `push: tags: v*` OR `workflow_dispatch` with `tag` |
| `scripts/release_version.py` | — | called by both workflows and locally |

Why two workflows: the `prepare-release` and `bump` jobs both push tags using `GITHUB_TOKEN`, which **does not fire `push` events** (anti-loop safeguard). They explicitly call `gh workflow run release.yml -f tag=…` to start `publish`. The `push: tags: v*` trigger remains as a fallback for hand-pushed tags.

### Local manual publish (escape hatch)

`Taskfile.yaml`'s `publish` task uses `$PYPI_TOKEN` from `.env`. Useful when CI is broken and you need to ship from a maintainer's machine. Prefer the automated path.
