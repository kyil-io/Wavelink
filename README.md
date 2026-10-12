<div align="center">


![Logo](https://raw.githubusercontent.com/PythonistaGuild/Wavelink/master/logo.png)

![Python Version](https://img.shields.io/pypi/pyversions/Wavelink)
[![PyPI - Version](https://img.shields.io/pypi/v/Wavelink)](https://pypi.org/project/wavelink/)
[![Github License](https://img.shields.io/github/license/PythonistaGuild/Wavelink)](LICENSE)
[![Lavalink Version](https://img.shields.io/badge/Lavalink-v4.0%2B-blue?color=%23FB7713)](https://lavalink.dev)
![Lavalink Plugins](https://img.shields.io/badge/Lavalink_Plugins-Native_Support-blue?color=%2373D673)


</div>


Wavelink is a robust and powerful Lavalink wrapper for [Discord.py](https://github.com/Rapptz/discord.py)
Wavelink features a fully asynchronous API that's intuitive and easy to use.


# Migrating from Version 2 to Version 3:

[Migrating Guide](https://wavelink.dev/en/latest/migrating.html)


### Features

- Full asynchronous design.
- Lavalink v4+ Supported with REST API.
- discord.py v2.0.0+ Support.
- Advanced AutoPlay and track recommendations for continuous play.
- Object orientated design with stateful objects and payloads.
- Fully annotated and complies with Pyright strict typing.


## Getting Started

**See Examples:** [Examples](https://github.com/PythonistaGuild/Wavelink/tree/main/examples)

**Lavalink:** [GitHub](https://github.com/lavalink-devs/Lavalink/releases), [Webpage](https://lavalink.dev)


## Documentation

[Official Documentation](https://wavelink.readthedocs.io/en/latest/)

## Support

For support using WaveLink, please join the official [Support Server](https://discord.gg/RAKc3HF) on
[Discord](https://discordapp.com)

[![Discord Banner](https://discordapp.com/api/guilds/490948346773635102/widget.png?style=banner2)](https://discord.gg/RAKc3HF)


## Installation

**WaveLink 3 requires Python 3.10+**

**Windows**


```sh
py -3.10 -m pip install -U wavelink
```

**Linux**

```sh
python3.10 -m pip install -U wavelink
```

**Virtual Environments**

```sh
pip install -U wavelink
```


## Lavalink

Wavelink **3** requires **Lavalink v4**.
See: [Lavalink](https://github.com/lavalink-devs/Lavalink/releases)

For spotify support, simply install and use [LavaSrc](https://github.com/topi314/LavaSrc) with your `wavelink.Playable`


### Notes

- Wavelink **3** is compatible with Lavalink **v4+**.
- Wavelink has built in support for Lavalink Plugins including LavaSrc and SponsorBlock.
- Wavelink is fully typed in compliance with Pyright Strict, though some nuances remain between discord.py and wavelink.

## Maintainer notes for this fork

- This fork accepts changes only through pull requests to `main`.
- GitHub Actions are intentionally disabled in repository settings for this fork.
- Required CI context `bootstrap/kyil-ci-trusted` is posted from an external,
  isolated maintainer-run job on `kyil-ci-01`/`kyil-ci-02`.
- Release publishing workflows are intentionally omitted from this fork baseline.
  Build and release operations are handled through reviewed maintainer procedures.

### External-only required CI procedure

From a clean checkout of the exact PR head SHA:

0. **Trust gate (mandatory before any install/import/build/test command):**

   - A trusted maintainer must explicitly select and audit the exact commit SHA to validate.
   - Do **not** run PR-controlled code on shared CI runners until that audit is complete.
   - A unique workspace and a Python venv are operational hygiene only; they are **not** a security sandbox.
   - Never schedule this automatically for untrusted external PRs.
   - External contributions must be fully reviewed and adopted onto a maintainer-owned branch first.
   - If code is still untrusted, validate only inside a disposable, credential-free VM (not on `kyil-ci-01`/`kyil-ci-02`).
   - Keep commit-status credentials scoped to the trusted controller path only.
   - Run as the existing unprivileged CI job user; do not use root for validation.

1. Create an isolated workspace (unique per run), create a venv **outside** the repository checkout, and install the CI toolchain:

   `python -m venv ../.venv-extci-wavelink && . ../.venv-extci-wavelink/bin/activate`

   `python -m pip install --upgrade pip`

   `python -m pip install -r requirements.txt ruff pyright build pytest`

2. Run validation:

   `tools/bootstrap/validate_external_ci.sh --mode normal`

3. For initial bootstrap PRs before lifecycle tests exist, run:

   `tools/bootstrap/validate_external_ci.sh --mode bootstrap-only`

   In this mode the helper prints `LIFECYCLE_SUITE=NOT PRESENT` (never "0 tests passed").

4. Post the result to GitHub commit status on the exact head SHA under context
   `bootstrap/kyil-ci-trusted`, including a link to the raw execution transcript.
