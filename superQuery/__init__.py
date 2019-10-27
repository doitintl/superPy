from .superQuery import Client

__all__ = [
    "Client"
]


def load_ipython_extension(ipython):
    """Called by IPython when this module is loaded as an IPython extension."""
    from .magics import _cell_magic
    from IPython.display import display_javascript

    ipython.register_magic_function(
        _cell_magic, magic_kind="cell", magic_name="superquery"
    )

    # Enable syntax highlighting
    js = "IPython.CodeCell.options_default.highlight_modes['magic_sql'] = {'reg':[/^%%superquery/]};"
    display_javascript(js, raw=True)
