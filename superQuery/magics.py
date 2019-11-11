__author__ = "mtagda"

"""IPython Magics
.. function:: %%superquery
    IPython cell magic to run a query and display the result as a DataFrame
    or store in a variable
    .. code-block:: python
        %%superquery [<destination_var>] [--project <project>] [--dry_run]
                   [--stats] [--dataset_id <dataset_id>] [--table <table>]
        <query>
    Parameters:
    * ``<destination_var>`` (optional, line argument):
        variable to store the query results. The results are not displayed if
        this parameter is used.
    * ``--project <project>`` (optional, line argument):
        Project to use for running the query.
    * ``--dry_run`` (optional, line argument):
        If this flag is used, a dry run is performed. Defaults to executing the
        query instead of dry run if this argument is not used
    * ``--dataset_id <dataset_id>`` (optional, line argument):
        Destination dataset.
    * ``--table <table>`` (optional, line argument):
        Destination table.
    * ``<query>`` (required, cell argument):
        SQL query to run.
    Returns:
        A :class:`pandas.DataFrame` with the query results."""

# import libraries
from .superQuery import *

LOGGER = setup_logging()

try:
    import IPython
    from IPython import display
    from IPython.core import magic_arguments
except ImportError:
    raise ImportError("This module can only be loaded in IPython.")


@magic_arguments.magic_arguments()
@magic_arguments.argument(
    "destination_var",
    nargs="?",
    help=("If provided, save the output to this variable instead of displaying it."),
)
@magic_arguments.argument(
    "--project",
    type=str,
    default=None,
    help=("Project to use for executing this query. Defaults to the context project."),
)
@magic_arguments.argument(
    "--dry_run",
    type=bool,
    default=False,
    help=(
            "Sets query to be a dry run."
            "Defaults to executing the query instead of dry run if this argument is not used."
    ),
)
@magic_arguments.argument(
    "--dataset_id",
    type=str,
    default=None,
    help=(
            "Select a destination dataset (optional)"
            "You must select both --dataset_id and --table in order to do that."
    ),
)
@magic_arguments.argument(
    "--table",
    type=str,
    default=None,
    help=(
            "Select a destination table (optional)"
    ),
)
@magic_arguments.argument(
    "--stats",
    type=bool,
    default=False,
    help=(
            "Set True if you want to see stats"
    ),
)
def _cell_magic(line, query):
    """Underlying function for superquery cell magic
    Note:
        This function contains the underlying logic for the 'superquery' cell
        magic. This function is not meant to be called directly.
    Args:
        line (str): "%%superquery" followed by arguments as required
        query (str): SQL query to run
    Returns:
        pandas.DataFrame: the query results.
    """

    # Initialize the client
    client = Client()
    # Strip input query
    QUERY = query.strip()

    # Parse arguments
    args = magic_arguments.parse_argstring(_cell_magic, line)

    if args.project:
        client.project(args.project)

    if args.table:
        client.table(args.table)

    if args.dataset_id:
        client.dataset(args.dataset_id)

    # Get query result
    try:
        result = client.query(QUERY, dry_run=args.dry_run)
    except Exception as ex:
        LOGGER.error("An error occurred")
        LOGGER.exception(ex)
        return

    # Print stats if needed
    if args.stats:
        print("---------STATISTICS---------")
        if (not result.stats.superParams["isDryRun"]):
            print("Total rows: ", result.stats.totalRows)
            print("Workflow: ", "Query")
            print("Cost: $ ", result.stats.superQueryTotalCost)
            print("Savings: % ", result.stats.saving)
            print("Cache used: ", result.stats.cacheUsed if hasattr(result.stats, "cacheUsed") else False)
            print("Cache type: ", result.stats.cacheType if hasattr(result.stats, "cacheUsed") else "None")
            print("DryRun: ", result.stats.superParams["isDryRun"])
        else:
            print("Workflow: ", "DryRun")
            print("Potential BQ bytes to scan: ", result.stats.bigQueryTotalBytesProcessed)
            print("Total rows: ", result.stats.totalRows)
            print("DryRun: ", result.stats.superParams["isDryRun"])

    if args.dry_run:
        return
    elif args.destination_var:
        IPython.get_ipython().push({args.destination_var: result.to_df()})
        return
    else:
        return result.to_df()
