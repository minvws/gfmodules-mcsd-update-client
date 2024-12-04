import argparse
import logging
from typing import Protocol, Any

import inject

import application

from cron.cleanup_expired import CleanupExpired

logger = logging.getLogger(__name__)


class CronCommand(Protocol):
    def init_arguments(self, subparser: Any) -> None:
        ...

    def run(self, args: argparse.Namespace) -> int:
        ...


CRON_COMMANDS: dict[str, CronCommand] = {
    "cleanup_expired": CleanupExpired,
}


def main() -> None:
    application.application_init()

    parser = argparse.ArgumentParser(description="Cron command line interface")
    subparser = parser.add_subparsers(dest="command", title="cron commands", help="valid cron commands", required=True)
    for name in CRON_COMMANDS.keys():
        command_get(name).init_arguments(subparser)

    args = parser.parse_args()

    # Run command
    logger.info("Running command %s", args.command)
    code = command_get(args.command).run(args)
    exit(code)


def command_exists(name: str) -> bool:
    return name in CRON_COMMANDS


def command_get(name: str) -> CronCommand:
    return inject.instance(CRON_COMMANDS[name])


if __name__ == "__main__":
    main()
