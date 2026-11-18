"""Command line entry point: ``sfemu serve``."""

from __future__ import annotations

import argparse
import logging

import uvicorn

from sfemu import __version__
from sfemu.server.app import create_app

DEFAULT_PORT = 8085


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="sfemu", description="Local Snowflake emulator.")
    parser.add_argument("--version", action="version", version=f"sfemu {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="run the emulator")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=DEFAULT_PORT)
    serve.add_argument(
        "--no-auto-create",
        action="store_true",
        help="do not create the database and schema named at login when they are missing",
    )
    serve.add_argument("--verbose", action="store_true", help="log every statement")
    args = parser.parse_args(argv)

    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("sfemu").setLevel(logging.DEBUG if args.verbose else logging.INFO)
    app = create_app(auto_create=not args.no_auto_create)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
