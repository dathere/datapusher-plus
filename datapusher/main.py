import os
import ckanserviceprovider.web as web

from datapusher import jobs
from config import config

# check whether jobs have been imported properly
assert jobs.push_to_datastore


def serve():
    web.init()
    web.app.run(config.get("HOST"), config.get("PORT"))


def serve_test():
    web.init()
    return web.app.test_client()


def main():
    import argparse

    argparser = argparse.ArgumentParser(
        description="Service that allows automatic migration of data to the CKAN DataStore"
    )

    argparser.add_argument(
        "config",
        metavar="CONFIG",
        type=argparse.FileType("r"),
        help="configuration file",
    )

    args = argparser.parse_args()

    os.environ["JOB_CONFIG"] = os.path.abspath(args.config.name)

    serve()


def initdb():
    import argparse

    argparser = argparse.ArgumentParser(description="Initializes the database")

    argparser.add_argument(
        "config",
        metavar="CONFIG",
        type=argparse.FileType("r"),
        help="configuration file",
    )

    args = argparser.parse_args()

    os.environ["JOB_CONFIG"] = os.path.abspath(args.config.name)

    import ckanserviceprovider.db as servicedb

    web._configure_app(web.app)
    servicedb.init(config.get("SQLALCHEMY_DATABASE_URI"))


if __name__ == "__main__":
    main()
