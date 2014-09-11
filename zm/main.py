
import sys

import docopt

import zm.server


HELP = """Usage:
    main <interface> <port>
"""


def main(argv=sys.argv[1:]):
    opts = docopt.docopt(HELP, argv=argv)
    server = zm.server.ZordzmanServer(
        (opts["<interface>"], int(opts["<port>"])))
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
