from ckanext.datapusher.cli import datapusher
from ckanext.datapusher_plus.model import init_tables


@datapusher.command()
def init_db():
    """Initialise the Datapusher Plus tables."""
    init_tables()
    print('Datapusher Plus tables created')

def get_commands():
    return [init_db]